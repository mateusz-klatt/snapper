"""Base class for market data publisher services.

Provides common functionality for ZeroMQ-based publishers that stream
real-time market data from exchanges.
"""

import asyncio
import collections
import contextlib
import math
import random
from abc import ABC
from abc import abstractmethod
from collections.abc import AsyncIterator
from collections.abc import Awaitable
from collections.abc import Callable
from dataclasses import dataclass
from dataclasses import field
from datetime import UTC
from datetime import datetime
from datetime import timedelta
from functools import partial
from time import monotonic
from time import perf_counter_ns
from typing import Any
from typing import Final
from typing import cast
from uuid import uuid7

import zmq
import zmq.asyncio
from loguru import logger
from sqlalchemy.exc import DBAPIError
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession

from snapper.application.process_manager.models import RegisterableProcess
from snapper.application.services.market_persist_policy import MarketDataType as PersistDataType
from snapper.application.services.market_persist_policy import MarketPersistPolicy
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
from snapper.data.repository_types import CandleRow
from snapper.data.repository_types import CandleUpsertRow
from snapper.data.repository_types import InstrumentFeedHealthUpsertRow
from snapper.data.repository_types import TickUpsertRow
from snapper.data.repository_types import TradeUpsertRow
from snapper.infrastructure.exchanges._subscription_health import _SymbolEntry
from snapper.infrastructure.exchanges.base import ExchangeClientBase
from snapper.infrastructure.exchanges.contracts import CandleUpdate
from snapper.infrastructure.exchanges.contracts import TickerUpdate
from snapper.infrastructure.exchanges.contracts import TradeUpdate
from snapper.infrastructure.network.egress_observability import EgressSnapshotPublisher
from snapper.infrastructure.network.egress_observability import resolve_egress_container_id
from snapper.infrastructure.network.egress_pool import get_egress_pool
from snapper.infrastructure.network.egress_pool import safely_initialize_egress_pool
from snapper.infrastructure.symbols.functions import get_market_data_capability_exclusions
from snapper.infrastructure.symbols.functions import resolve_symbol_public_id
from snapper.infrastructure.symbols.mapper import SymbolMapperService
from snapper.messaging.infrastructure.publisher import MessagePublisher
from snapper.messaging.infrastructure.publisher import SequenceTracker
from snapper.messaging.infrastructure.tick_probe import get_probe
from snapper.messaging.infrastructure.trade_probe import get_probe as get_trade_probe
from snapper.messaging.infrastructure.validated_socket import HWM_MARKET_DATA
from snapper.messaging.infrastructure.validated_socket import ValidatedPublisher
from snapper.messaging.infrastructure.validated_socket import ValidatedSubscriber
from snapper.messaging.infrastructure.validated_socket import apply_hwm
from snapper.messaging.publishers.candle_aggregator import SUPPORTED_SYNTHESIS_TIMEFRAMES
from snapper.messaging.publishers.candle_aggregator import CandleAggregator
from snapper.messaging.publishers.candle_aggregator import LateCandleDrop
from snapper.messaging.publishers.candle_aggregator import SeededIncompleteWindow
from snapper.messaging.publishers.native_candle_finalizer import NativeCandleFinalizer
from snapper.messaging.publishers.native_candle_finalizer import window_seconds
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

_CANDLE_WRITE_QUEUE_MAX = 20_000
_CANDLE_WRITER_DROP_LOG_INTERVAL_S = 1.0
_CANDLE_WRITER_SHUTDOWN_POLL_S = 0.5
_candle_writer_drop_counters: dict[str, list[float]] = {}

_TRADE_WRITE_QUEUE_MAX = 5_000
_TRADE_WRITER_DROP_LOG_INTERVAL_S = 1.0
_TRADE_WRITER_SHUTDOWN_POLL_S = 0.5
_trade_writer_drop_counters: dict[str, list[float]] = {}
_TRADE_ID_LRU_MAX_PER_SYMBOL: int = 1000
"""Bounded per-symbol LRU cache size for trade-id deduplication."""

_CONSUMER_RESTART_BACKOFF_S = 2.0
_LIVENESS_RECOVERY_THRESHOLD_S_DEFAULT = 300
_MIN_RECOVERY_INTERVAL_S = 60.0
_RECOVERY_BACKOFF_INITIAL_S: Final = 1.0
"""First sleep between failed liveness-recovery attempts."""
_RECOVERY_BACKOFF_CAP_S: Final = 60.0
"""Upper bound on the capped-exponential backoff between recovery attempts.

Recovery never abandons a dark feed: it keeps rebuilding the venue
connection with this backoff ceiling until fresh messages resume or the
publisher stops. This replaces the previous single ``asyncio.wait_for``
attempt bounded at 90 seconds, which cancelled a Spot reconnect mid
subscription-replay (~195 s of work) on every cycle and left the public
feeds dark for hours after a multi-minute network outage."""
_RECOVERY_ATTEMPT_TIMEOUT_S: Final = 180.0
"""Upper bound on a SINGLE liveness-recovery attempt.

The 2026-06-09 blackout fault test proved an attempt can hang forever
OUTSIDE the venue-level connect timeout (a subscription replay queued
behind a wedged subscribe lock), and a hung attempt holds the recovery
lock — silencing every future liveness trigger while the feed stays
connected-but-dark until a process restart. Bounding the whole attempt
turns any unforeseen hang into ``recovery attempt N raised TimeoutError``
plus a retry. Attempts are idempotent (subscription caches survive and
replay re-marks tracker entries with preserved budgets), and 180 s gives
~3.6x headroom over the worst venue's healthy attempt (~50 s of paced
Futures replay plus rate-limit cooldowns) — the bound MUST exceed a
healthy attempt or recovery would churn half-replayed connections
forever."""
_RECOVERY_JITTER_FRACTION: Final = 0.2
"""Plus/minus fractional jitter applied to each recovery backoff so
independent publishers do not synchronise their reconnect attempts."""
_RECOVERY_PROGRESS_GRACE_S: Final = 45.0
"""Window to observe for fresh messages after a recovery attempt before
treating the attempt as unsuccessful and backing off for another try.

Long enough for a successful reconnect plus paced subscription replay to
start delivering data; a failed reconnect resumes the backoff loop as
soon as the grace window elapses with no progress."""
_RECOVERY_PROGRESS_POLL_S: Final = 1.0
"""Poll cadence while observing for post-recovery message progress."""
_CANDLE_RECOVERY_PROGRESS_GRACE_S: Final = 120.0
"""Window to observe for a fresh native candle after a candle-triggered recovery.

A candle-triggered recovery rebuilds the socket and replays the OHLC
subscriptions, but the first candle only arrives at the next minute boundary,
so this grace must exceed the full-universe replay time plus that ~60 s
inter-bar latency. Reusing the shorter message grace would declare the attempt
unsuccessful before any candle could prove recovery and churn full WS restarts
(trades alone advance the message watermark, so candle progress must be observed
separately)."""
_DARK_FEED_EXIT_CEILING_S: Final = 1500.0
"""Maximum continuous message-silence (seconds) a publisher tolerates before
escalating from in-process recovery to a fatal process exit.

Persistent recovery (:meth:`MarketDataPublisherService._run_recovery_under_lock`)
handles the common case in process; this backstop only fires when a feed stays
dark far longer than any real reconnect needs — e.g. a wedged SDK or a recovery
bug. Exiting lets the process launcher respawn a fresh subprocess under its
crash-loop guards. Set well above the per-venue liveness thresholds (60-120 s)
so a normal multi-minute outage recovers in process without a restart.

INVARIANT: this MUST stay strictly greater than the launcher's
``_TOTAL_RESET_UPTIME_S`` (1200 s). A dark-exit process has run for at least
this ceiling before exiting, so a larger value guarantees its uptime exceeds
the launcher's long-healthy threshold — the launcher then treats every
dark-exit as a long-healthy death and resets the lifetime failed-restart
counter, so a genuinely prolonged outage produces an UNBOUNDED slow
restart-and-retry cadence (self-healing the instant connectivity returns)
rather than exhausting the lifetime restart budget and permanently abandoning
the feed (which would recreate the multi-hour dark-feed incident this work
fixes). The invariant is asserted in the test suite."""

_FEED_HEALTH_FLUSH_INTERVAL_S = 30.0
"""Cadence for persisting the subscription-health snapshot to the DB.

The publisher owns its exchange client's in-memory
:class:`SubscriptionHealthTracker`, which is lost on restart. Every
``_FEED_HEALTH_FLUSH_INTERVAL_S`` the publisher snapshots that tracker,
converts the tracker's monotonic-clock fields to wall-clock, and upserts
the current state into ``instrument_feed_health`` so operators can query
which symbols are dark, when each last received data, and why — after the
fact."""

_CANDLE_FLUSH_INTERVAL_S = 30.0
"""Cadence for the time-driven higher-TF candle flush (Phase 1b forward-fill).

Only started when ``candle_forward_fill`` is enabled. Well under the 300s
smallest synthesizable timeframe, so a wall-clock-sealed or forward-filled
higher-TF bar is at most ~one interval late."""

_CANDLE_FLUSH_GRACE_S = 5.0
"""Wall-clock slack the flush subtracts from ``now`` before sealing a minute/window.

Gives the venue a few seconds past a minute boundary to deliver its FINAL frame
for the just-ended minute before the flush finalizes it — otherwise that late
frame is dropped and the higher-TF bar is sealed with a stale value. Comfortably
covers typical kraken OHLC frame latency while keeping a forward-filled bar at most
~one flush interval plus this grace late."""

_CANDLE_REPAIR_DEBOUNCE_S: Final = 1.0
"""Debounce interval for coalescing late 1m corrections into one repair key."""

_CANDLE_REPAIR_MAX_CONCURRENCY: Final = 4
"""Maximum concurrent durable re-derivation groups run by the repair loop."""

_CANDLE_REPAIR_PENDING_MAX: Final = 100_000
"""Maximum number of pending synthesized-candle repair keys retained in memory."""

_CANDLE_REPAIR_MAX_ATTEMPTS: Final = 3
"""Maximum repair attempts before abandoning a permanently unavailable source window."""

_SEEDED_INCOMPLETE_REPAIR_MAX_ATTEMPTS: Final = 60
"""Maximum source-gated seeded repair attempts before manual backfill warning."""

_PERSIST_SKIPPED_LOG_INTERVAL_S = 60.0
"""Cadence for the rate-limited ``persist_skipped_total`` log line.

Drops to disk are an expected condition (every wildcard tick that
isn't covered by a scope grant or explicit allowlist), so we want a
rough volume estimate in the logs but not one entry per skip."""

_WRITER_RECONNECT_INITIAL_BACKOFF_S: Final = 1.0
"""First sleep after a writer-session-lost recovery starts."""

_WRITER_RECONNECT_MAX_BACKOFF_S: Final = 30.0
"""Upper bound on the exponential backoff between writer session re-opens."""

_DISCONNECT_HINTS: Final = (
    "connection refused",
    "connection reset",
    "shutting down",
    "server closed the connection",
    "no connection",
    "broken pipe",
)
"""Substrings on a stringified exception that indicate the underlying DB
connection died (Postgres restart, network reset, fwall idle-out) rather
than a query-level problem (constraint violation, syntax error, timeout
on a still-live connection)."""


class FeedDarkTooLongError(RuntimeError):
    """Raised by the heartbeat loop when a feed stays dark past the exit ceiling.

    Persistent in-process recovery handles ordinary outages; this signals
    that a feed has produced no messages for longer than
    ``_DARK_FEED_EXIT_CEILING_S`` despite recovery, so the publisher should
    exit non-zero and let the process launcher respawn a fresh subprocess
    under its crash-loop guards. The heartbeat loop re-raises it explicitly
    past its generic ``except Exception`` handler so it is never swallowed.
    """


class _WriterSessionLostError(Exception):
    """Pinned writer session lost its underlying DB connection.

    The writer loop catches this to dispose the dead session and
    re-enter ``_open_*_writer_session()`` so a fresh connection is
    acquired. Separate from generic flush errors so transient SQL
    failures (constraint violations on a still-live session, timeouts
    that did not break the connection) do not trigger session churn.
    """


def _is_disconnect_error(exc: BaseException) -> bool:
    """Return ``True`` when ``exc`` indicates a lost DB connection.

    Combines SQLAlchemy's :attr:`DBAPIError.connection_invalidated`
    flag (the pool sets it when it has already invalidated the handle)
    with substring matches on common asyncpg / libpq disconnect
    messages. Conservative on purpose -- false negatives just mean we
    retry once more before recycling the session; false positives would
    force unnecessary pool churn on every transient error.

    Args:
        exc: The exception raised inside a writer flush.

    Returns:
        ``True`` when the writer loop should dispose the pinned session
        and re-open via :meth:`_open_*_writer_session`.
    """
    if isinstance(exc, DBAPIError) and getattr(exc, "connection_invalidated", False):
        return True
    msg = str(exc).lower()
    return any(hint in msg for hint in _DISCONNECT_HINTS)


_DATA_TYPES_FOR_RAIL: tuple[PersistDataType, ...] = ("ticks", "trades", "candles")
"""Data types checked by the publisher-start safety rail."""

_WriterQueue = (
    asyncio.Queue[TickUpsertRow] | asyncio.Queue[CandleUpsertRow] | asyncio.Queue[TradeUpsertRow]
)
type _WriterRow = TickUpsertRow | CandleUpsertRow | TradeUpsertRow
type _WriterFlush[WriterRow: _WriterRow] = Callable[[list[WriterRow]], Awaitable[None]]


@dataclass
class _WriterBatchState[WriterRow]:
    """Mutable writer batch state shared across session reconnects.

    Attributes:
        batch: Rows currently buffered by a writer loop.
        started_at: Event-loop timestamp when the current batch was opened.
    """

    batch: list[WriterRow]
    started_at: float | None = None


@dataclass(frozen=True)
class _CandleRepairKey:
    """Natural key of a synthesized candle that must be re-derived."""

    symbol: str
    timeframe: str
    window_begin: datetime


@dataclass
class _PendingCandleRepair:
    """Repair key state retained until its source correction is committed."""

    required_sequence: int | None = None
    attempts: int = 0
    expected_minute_count: int | None = None
    expected_source_minutes: frozenset[datetime] | None = None
    last_observed_minute_count: int | None = None


@dataclass(frozen=True)
class _CandleRepairResult:
    """Result of attempting one pending synthesized-candle repair."""

    key: _CandleRepairKey
    required_sequence: int | None
    done: bool


@dataclass(frozen=True)
class _LateCandleDropKey:
    """Natural key of a late 1m drop emitted by the pure aggregator."""

    symbol: str
    minute: datetime


@dataclass
class _PendingLateCandleDrop:
    """Late-drop state waiting for a matching committed correction row."""

    write_sequence: int | None = None
    repair_keys: set[_CandleRepairKey] = field(default_factory=set)


@dataclass(frozen=True)
class _EnqueuedCandleWrite:
    """Candle writer queue row with its publisher-assigned write sequence."""

    native_symbol: str
    row: CandleUpsertRow
    sequence: int


type TickPayloadValue = float | bool | None
"""Union of every value type in the tick payload deduplication tuple."""


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
) -> CandleUpsertRow | None:
    """Put a candle row on the writer queue, dropping the oldest if full.

    Mirrors :func:`_enqueue_or_drop_oldest_tick_write`; see that
    docstring for the rationale. The drop log carries a
    ``candle-writer`` label so operators can distinguish it from
    upstream WS-queue drops and from tick-writer drops.

    Args:
        queue: Bounded writer queue.
        row: Candle row to enqueue for persistence.
        label: Human-readable label (typically the exchange name).

    Returns:
        The evicted row when the queue was full, otherwise ``None``.
    """
    try:
        queue.put_nowait(row)
        return None
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
        queue.put_nowait(row)
        return evicted


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
        self._last_tick_payload: dict[str, tuple[TickPayloadValue, ...]] = {}
        self._seen_trade_ids: dict[str, collections.OrderedDict[str, None]] = {}
        self._last_message_at: float = monotonic()
        self._last_candle_msg_at: float = monotonic()
        self._consumes_native_candles: bool = False
        self._recovery_lock: asyncio.Lock = asyncio.Lock()
        self._last_recovery_at: float = 0.0
        self._recovery_tasks: set[asyncio.Task[None]] = set()
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
        self._candle_aggregator: CandleAggregator | None = None
        self._candle_flush_loop_task: asyncio.Task[None] | None = None
        self._candle_repair_loop_task: asyncio.Task[None] | None = None
        self._pending_candle_repairs: dict[_CandleRepairKey, _PendingCandleRepair] = {}
        self._pending_late_candle_drops: dict[_LateCandleDropKey, _PendingLateCandleDrop] = {}
        self._repair_drop_key_by_repair_key: dict[_CandleRepairKey, _LateCandleDropKey] = {}
        self._candle_repair_event: asyncio.Event = asyncio.Event()
        self._next_candle_write_sequence: int = 0
        self._candle_write_sequence_by_row_id: dict[int, int] = {}
        self._committed_candle_write_sequences: set[int] = set()
        self._candle_shutdown_repair_drain_active: bool = False
        self._native_finalizer: NativeCandleFinalizer | None = None
        self._native_finalize_flush_task: asyncio.Task[None] | None = None
        self._persist_intermediate_candles: bool = False
        self._candle_writer_task: asyncio.Task[None] | None = None
        self._candle_writer_session: AsyncSession | None = None
        self._trade_write_queue: asyncio.Queue[TradeUpsertRow] = asyncio.Queue(
            maxsize=_TRADE_WRITE_QUEUE_MAX
        )
        self._trade_consumer_task: asyncio.Task[None] | None = None
        self._trade_writer_task: asyncio.Task[None] | None = None
        self._trade_writer_session: AsyncSession | None = None
        self._persist_policy: MarketPersistPolicy | None = None
        self._persist_skipped_counters: dict[tuple[str, PersistDataType], list[float]] = {}
        self._feed_health_loop_task: asyncio.Task[None] | None = None
        self._extra_background_tasks: list[asyncio.Task[None]] = []
        self._egress_snapshot_publisher: EgressSnapshotPublisher | None = None

    def _require_repository(self) -> Repository:
        """Return initialized repository or raise an explicit runtime error.

        Returns:
            Initialized repository instance.

        Raises:
            RuntimeError: If repository setup has not completed.
        """
        repository = self.repository
        if repository is None:
            raise RuntimeError(_REPO_NOT_INIT_MSG)
        return repository

    def set_persist_policy(self, policy: MarketPersistPolicy | None) -> None:
        """Inject the :class:`MarketPersistPolicy` for selective DB-write gating.

        Called from the process-launcher path once the FastAPI lifespan
        has built the singleton policy. A ``None`` policy degrades the
        publisher to "persist everything" (legacy / test-mode behaviour)
        so tests + standalone subprocess paths that don't carry a
        policy reference continue to write every row.

        Args:
            policy: Configured :class:`MarketPersistPolicy` or ``None``
                to clear / unset.
        """
        self._persist_policy = policy

    def _should_persist_row(
        self,
        data_type: PersistDataType,
        exchange: AllExchange,
        native_symbol: str,
    ) -> bool:
        """Return ``True`` when the row should land on the DB writer queue.

        Wraps :meth:`MarketPersistPolicy.should_persist` with a rate-
        limited drop counter so operators can monitor how many rows
        the policy filters out per exchange + data type. Missing
        policy degrades to "allow all" so legacy paths stay green.

        Args:
            data_type: ``"ticks"``, ``"trades"``, or ``"candles"``.
            exchange: Source exchange for the row.
            native_symbol: Native symbol associated with the row.

        Returns:
            ``True`` when the row should be persisted; ``False`` when
            the policy filtered it out (DB write is skipped, ZMQ
            publish already happened upstream).
        """
        policy = self._persist_policy
        if policy is None:
            return True
        if policy.should_persist(exchange, data_type, native_symbol):
            return True
        self._record_persist_skip(exchange, data_type)
        return False

    def _verify_persist_policy_safety_rail(self, process_name: str) -> None:
        """Refuse to start if wildcard + auto + empty scope + empty extra.

        Hard-fails publisher startup when every condition holds at
        once for any data type the publisher emits — a misconfig that
        would silently drop every market-data write while pretending to
        be operating normally:

        1. ``parameters.symbols`` is the literal wildcard ``["*"]``.
        2. The policy mode for ``(exchange, data_type)`` is ``"auto"``.
        3. The wallet-scope-derived set for the exchange is empty.
        4. The ``market_persist_extra`` overlay for ``(exchange, data_type)``
           is also empty.

        Each data type is checked independently; the first failure
        raises ``RuntimeError`` so the operator sees the exact
        ``(exchange, data_type)`` that needs configuration. The check
        is skipped when a policy has not been injected (legacy / test
        / standalone subprocess paths).

        Args:
            process_name: Process name for the error message.

        Raises:
            RuntimeError: When the four conditions all hold.
        """
        policy = self._persist_policy
        if policy is None:
            return
        if self._input_symbols != ["*"]:
            return
        exchange = self._get_exchange_name()
        scope_set = policy.wallet_scope_pairs_for(exchange)
        for data_type in _DATA_TYPES_FOR_RAIL:
            mode = policy.mode_for(exchange, data_type)
            if mode != "auto":
                continue
            extra_set = policy.extra_for(exchange, data_type)
            if scope_set or extra_set:
                continue
            raise RuntimeError(
                f"Publisher {process_name} configured with wildcard symbols but "
                f"persist policy resolves to zero instruments for {exchange}/{data_type}. "
                f"Either narrow parameters.symbols, add a scope grant, set "
                f"mode=explicit with an exchanges allowlist, or populate "
                f"market_persist_extra[{data_type}][{exchange}]."
            )

    def _record_persist_skip(self, exchange: AllExchange, data_type: PersistDataType) -> None:
        """Increment + rate-limit log the ``persist_skipped_total`` counter."""
        key = (str(exchange), data_type)
        counters = self._persist_skipped_counters.setdefault(key, [0.0, 0.0])
        counters[0] += 1
        now = monotonic()
        if now - counters[1] >= _PERSIST_SKIPPED_LOG_INTERVAL_S:
            logger.info(
                "persist_skipped_total exchange={} data_type={} count={} window_s={:.1f}",
                exchange,
                data_type,
                int(counters[0]),
                now - counters[1],
            )
            counters[0] = 0.0
            counters[1] = now

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

    def _log_capability_exclusions(self, exchange_name: AllExchange) -> None:
        """Log the once-per-startup market-data capability exclusion summary.

        Only wildcard publishers (``self._input_symbols == ["*"]``) report
        exclusions: a wildcard publisher subscribes the full market-data
        universe, so symbols whose capability row carries
        ``can_market_data = False`` are genuinely withheld from a universe the
        operator asked to cover in full. An explicit-symbol publisher only
        subscribes its own list, so the "excluded from the subscribed
        universe" concept does not apply and must stay quiet. When there are
        exclusions, one INFO line reports the counts and one DEBUG line lists
        the sorted excluded native symbols. Nothing is logged when there are
        no exclusions so healthy startups stay quiet.

        Args:
            exchange_name: Resolved exchange identifier used for the lookup
                and the log line.

        Returns:
            None.
        """
        if self._input_symbols != ["*"]:
            return
        included, excluded = get_market_data_capability_exclusions(exchange_name)
        if not excluded:
            return
        logger.info(
            "{}: {} symbol(s) excluded from market data by capability "
            "(can_market_data=False); {} included",
            exchange_name,
            len(excluded),
            len(included),
        )
        logger.debug("{}: market-data-excluded symbols: {}", exchange_name, excluded)

    async def _maybe_init_egress_pool(self, settings_service: SettingsService) -> None:
        """Initialize the egress pool in this publisher process when enabled.

        The egress pool is a process-local singleton; the API/coordinator
        process initializes its own, but feed publishers run in separate
        subprocesses, so each must initialize the pool here for the Kraken
        connect shim and Walutomat's pooled HTTP transport to route through the
        configured tunnels. Gated on ``feed_egress_enabled`` (default off);
        when off the feeds keep their direct-to-exchange connections.

        Args:
            settings_service: The publisher's initialized settings service.

        Returns:
            None.
        """
        if not self.settings.feed_egress_enabled:
            return
        await safely_initialize_egress_pool(settings_service)
        logger.info(
            f"{self._get_process_name()}: egress pool initialization attempted "
            "(feed_egress_enabled) — confirm routing via `ss` (a malformed/absent "
            "egress_pool setting leaves the pool empty and feeds stay direct)"
        )

    def _start_egress_snapshot_publisher(self, process_name: str) -> None:
        """Start egress snapshot publishing when this process owns a pool.

        Args:
            process_name: Existing publisher process label used for logging.
        """
        if get_egress_pool() is None or self.msg_publisher is None:
            return
        publisher = EgressSnapshotPublisher(
            container=resolve_egress_container_id(process_name),
            publisher=self.msg_publisher,
            interval_seconds=self.settings.zmq_heartbeat_interval_ms / 1000.0,
        )
        publisher.start()
        self._egress_snapshot_publisher = publisher

    async def _stop_egress_snapshot_publisher(self) -> None:
        """Stop the egress snapshot publisher if it was started."""
        publisher = self._egress_snapshot_publisher
        self._egress_snapshot_publisher = None
        if publisher is None:
            return
        await publisher.stop()

    async def _configure_candle_consumers(
        self, symbols_to_subscribe: list[str], tasks: list[asyncio.Task[None]]
    ) -> list[str]:
        """Given runtime settings, when starting, then configure candle consumers.

        Args:
            symbols_to_subscribe: Symbols selected after exchange connection limits.
            tasks: Mutable startup task list that receives any flush tasks.

        Returns:
            Candle timeframes that should get supervised consumer tasks.

        Raises:
            Exception: Propagates restart-seed repository failures unchanged.
        """
        timeframes = self.settings.timeframes
        higher_timeframes = [timeframe for timeframe in timeframes if timeframe != "1m"]
        supported_higher = [tf for tf in higher_timeframes if tf in SUPPORTED_SYNTHESIS_TIMEFRAMES]
        self._candle_aggregator = None
        if supported_higher:
            return await self._configure_synthesized_candle_consumers(
                symbols_to_subscribe, higher_timeframes, supported_higher, tasks
            )
        return self._configure_native_candle_consumers(timeframes)

    async def _configure_synthesized_candle_consumers(
        self,
        symbols_to_subscribe: list[str],
        higher_timeframes: list[str],
        supported_higher: list[str],
        tasks: list[asyncio.Task[None]],
    ) -> list[str]:
        """Given higher timeframes, when synthesis is enabled, then start its support.

        Args:
            symbols_to_subscribe: Symbols selected after exchange connection limits.
            higher_timeframes: Configured timeframes above ``1m``.
            supported_higher: Higher timeframes supported by the aggregator.
            tasks: Mutable startup task list that receives the optional flush task.

        Returns:
            The native ``1m`` consumer timeframe required for synthesis.

        Raises:
            Exception: Propagates restart-seed repository failures unchanged.
        """
        unsupported = [tf for tf in higher_timeframes if tf not in SUPPORTED_SYNTHESIS_TIMEFRAMES]
        if unsupported:
            logger.warning(
                f"{self.__class__.__name__}: configured timeframes {unsupported} are not "
                f"synthesizable and will NOT be published (supported higher TFs: "
                f"{sorted(SUPPORTED_SYNTHESIS_TIMEFRAMES)})"
            )
        native_replaced = [tf for tf in supported_higher if tf in self._native_candle_timeframes()]
        if native_replaced:
            logger.warning(
                f"{self.__class__.__name__}: synthesizing {native_replaced} from the 1m stream "
                f"INSTEAD of the venue-native OHLC feed (intentional per the synthesize-from-1m "
                f"design; the rolled-up VWAP/trades approximate the native bar)"
            )
        forward_fill = self.settings.candle_forward_fill and self._supports_forward_fill()
        if self.settings.candle_forward_fill and not self._supports_forward_fill():
            logger.warning(
                f"{self.__class__.__name__}: candle_forward_fill is set but this venue is not a "
                f"continuous-corpus feed — forward-fill is forced OFF (it would manufacture bars "
                f"the strategy was never validated on)"
            )
        self._candle_aggregator = CandleAggregator(
            supported_higher,
            forward_fill=forward_fill,
            flush_grace_seconds=_CANDLE_FLUSH_GRACE_S,
        )
        await self._seed_aggregator_from_db(
            symbols_to_subscribe, supported_higher, datetime.now(UTC)
        )
        self._candle_aggregator.set_live_epoch(self._candle_live_epoch())
        self._candle_repair_loop_task = asyncio.create_task(
            self._candle_repair_loop(self._get_data_exchange())
        )
        tasks.append(self._candle_repair_loop_task)
        if self._candle_aggregator.forward_fill:
            self._candle_flush_loop_task = asyncio.create_task(
                self._candle_flush_loop(self._get_data_exchange())
            )
            tasks.append(self._candle_flush_loop_task)
        return ["1m"]

    def _configure_native_candle_consumers(self, timeframes: list[str]) -> list[str]:
        """Given configured timeframes, when no synthesis runs, then select native feeds.

        Args:
            timeframes: Configured candle timeframes.

        Returns:
            Timeframes supported by the exchange's native candle stream.

        Raises:
            This helper does not raise directly.
        """
        stream_timeframes = self._candle_stream_timeframes()
        candle_consumer_timeframes = [tf for tf in timeframes if tf in stream_timeframes]
        dropped = [tf for tf in timeframes if tf not in stream_timeframes]
        if dropped:
            logger.warning(
                f"{self.__class__.__name__}: timeframes {dropped} are neither natively "
                f"subscribable nor synthesized by this publisher and will NOT be published "
                f"(native: {sorted(stream_timeframes)})"
            )
        return candle_consumer_timeframes

    def _start_native_finalizer_if_needed(
        self, candle_consumer_timeframes: list[str], tasks: list[asyncio.Task[None]]
    ) -> None:
        """Given candle consumers, when live candles run, then start finalization.

        Args:
            candle_consumer_timeframes: Candle timeframes selected for consumption.
            tasks: Mutable startup task list that receives the finalizer task.

        Returns:
            None.

        Raises:
            This helper does not raise directly.
        """
        self._consumes_native_candles = bool(candle_consumer_timeframes)
        self._native_finalizer = None
        if self._consumes_native_candles:
            self._persist_intermediate_candles = self.settings.persist_intermediate_candles
            self._native_finalizer = NativeCandleFinalizer(
                persist_intermediate=self._persist_intermediate_candles,
                flush_grace_seconds=_CANDLE_FLUSH_GRACE_S,
            )
            self._native_finalize_flush_task = asyncio.create_task(
                self._native_finalize_flush_loop(self._get_data_exchange())
            )
            tasks.append(self._native_finalize_flush_task)

    def _start_trade_tasks_if_supported(
        self, symbols_to_subscribe: list[str], tasks: list[asyncio.Task[None]], process_name: str
    ) -> None:
        """Given selected symbols, when trades are supported, then start trade tasks.

        Args:
            symbols_to_subscribe: Symbols selected after exchange connection limits.
            tasks: Mutable startup task list that receives trade tasks.
            process_name: Log label for this publisher process.

        Returns:
            None.

        Raises:
            This helper does not raise directly.
        """
        if self._supports_public_trades():
            self._trade_consumer_task = asyncio.create_task(
                self._supervise_consumer("trade", partial(self._trade_loop, symbols_to_subscribe))
            )
            tasks.append(self._trade_consumer_task)
            self._trade_writer_task = asyncio.create_task(self._trade_writer_loop())
            tasks.append(self._trade_writer_task)
        else:
            logger.info(f"{process_name}: Trade loop disabled (exchange has no public trade feed)")

    def _symbols_for_trade_loop(self, symbols: list[str]) -> list[str]:
        """Return symbols that should be subscribed by the trade loop.

        Args:
            symbols: Symbols selected for this publisher instance.

        Returns:
            Symbols to pass into the exchange client's trade subscription.
        """
        return symbols

    async def start(self) -> None:
        """Start the publisher service and connect to exchange."""
        exchange_name = self._get_exchange_name()
        process_name = self._get_process_name()
        set_log_context(process_name)
        if self.running:
            logger.warning(f"{process_name}: Already running")
            return
        self._log_capability_exclusions(exchange_name)
        bootstrap_settings = get_settings()
        settings_service = await get_settings_service(
            bootstrap_settings.db_url,
            bootstrap_settings.zmq_broker_xsub,
        )
        self.settings = get_settings_with_service(settings_service)
        logger.info(f"{process_name}: AppSettings initialized with database access")
        await self._maybe_init_egress_pool(settings_service)
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
        self._verify_persist_policy_safety_rail(process_name)
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
        self._exchange_client.start_health_loop()
        logger.info(f"{process_name}: Exchange client connected (anonymous, public data)")
        self.running = True
        tasks: list[asyncio.Task[None]] = []
        tasks.append(asyncio.create_task(self._heartbeat_loop()))
        tasks.append(asyncio.create_task(self._symbol_aliases_loop()))
        self._feed_health_loop_task = asyncio.create_task(self._feed_health_flush_loop())
        tasks.append(self._feed_health_loop_task)
        self._start_egress_snapshot_publisher(process_name)
        symbols_to_subscribe = self.symbols[:max_symbols] if max_symbols > 0 else self.symbols
        candle_consumer_timeframes = await self._configure_candle_consumers(
            symbols_to_subscribe, tasks
        )
        self._start_native_finalizer_if_needed(candle_consumer_timeframes, tasks)
        self._candle_consumer_tasks = [
            asyncio.create_task(
                self._supervise_consumer(
                    f"candle:{timeframe}",
                    partial(self._candle_loop, symbols_to_subscribe, timeframe),
                )
            )
            for timeframe in candle_consumer_timeframes
        ]
        tasks.extend(self._candle_consumer_tasks)
        self._candle_writer_task = asyncio.create_task(self._candle_writer_loop())
        tasks.append(self._candle_writer_task)
        self._tick_consumer_task = asyncio.create_task(
            self._supervise_consumer("tick", partial(self._tick_loop, symbols_to_subscribe))
        )
        tasks.append(self._tick_consumer_task)
        self._tick_writer_task = asyncio.create_task(self._tick_writer_loop())
        tasks.append(self._tick_writer_task)
        self._start_trade_tasks_if_supported(symbols_to_subscribe, tasks, process_name)
        self._extra_background_tasks = await self._start_extra_background_tasks(
            symbols_to_subscribe
        )
        tasks.extend(self._extra_background_tasks)
        try:
            await asyncio.gather(*tasks)
        except asyncio.CancelledError:
            logger.info(f"{process_name}: Tasks cancelled")
            raise

    async def stop(self) -> None:
        """Stop the publisher service and disconnect from exchange.

        Shutdown ordering:

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
        if (
            not self.running
            and not self._has_pending_writer_shutdown()
            and not self._recovery_tasks
            and not self._extra_background_tasks
        ):
            return
        self.running = False
        self._candle_shutdown_repair_drain_active = self._candle_writer_can_drain()
        try:
            recovery_tasks = list(self._recovery_tasks)
            for task in recovery_tasks:
                task.cancel()
            if recovery_tasks:
                await asyncio.gather(*recovery_tasks, return_exceptions=True)
                self._recovery_tasks.difference_update(recovery_tasks)
            if self._exchange_client is not None:
                await self._exchange_client.stop_health_loop()
            await self._stop_egress_snapshot_publisher()
            await self._stop_feed_health_loop()
            await self._stop_extra_background_tasks()
            await self._stop_tick_pipeline()
            await self._stop_candle_pipeline()
            await self._stop_trade_pipeline()
            await self._close_runtime_resources()
            exchange_name = self._get_exchange_name()
            logger.info(f"{exchange_name}_feed_publisher: Stopped")
        finally:
            self._candle_shutdown_repair_drain_active = False

    async def _start_extra_background_tasks(
        self, symbols_to_subscribe: list[str]
    ) -> list[asyncio.Task[None]]:
        """Start subclass-owned background tasks after the standard pipelines.

        Args:
            symbols_to_subscribe: The symbols selected for this publisher
                connection after venue-level limits have been applied.

        Returns:
            Background tasks that should join the main gather and be cancelled
            during stop. The base implementation has no extra work.
        """
        completed: asyncio.Future[list[asyncio.Task[None]]] = (
            asyncio.get_running_loop().create_future()
        )
        completed.set_result([])
        return await completed

    async def _stop_extra_background_tasks(self) -> None:
        """Cancel and await subclass-owned background tasks.

        Returns:
            None.
        """
        for task in self._extra_background_tasks:
            task.cancel()
        await self._await_shutdown_tasks(self._extra_background_tasks)
        self._extra_background_tasks = []

    def _has_pending_writer_shutdown(self) -> bool:
        """Return whether stop should drain a partially initialised writer."""
        return (
            self._writer_shutdown_pending(self._tick_writer_task, self._tick_write_queue)
            or self._writer_shutdown_pending(self._candle_writer_task, self._candle_write_queue)
            or self._writer_shutdown_pending(self._trade_writer_task, self._trade_write_queue)
        )

    def _writer_shutdown_pending(
        self, task: asyncio.Task[None] | None, queue: _WriterQueue | None
    ) -> bool:
        """Return whether a writer task or queued rows remain during shutdown."""
        return task is not None or (queue is not None and not queue.empty())

    async def _await_shutdown_task(self, task: asyncio.Task[None] | None) -> None:
        """Await a shutdown task while preserving best-effort stop semantics."""
        if task is None:
            return
        with contextlib.suppress(asyncio.CancelledError, Exception):
            await task

    async def _await_shutdown_tasks(self, tasks: list[asyncio.Task[None]]) -> None:
        """Await shutdown tasks in start order."""
        for task in tasks:
            await self._await_shutdown_task(task)

    async def _join_shutdown_queue(self, queue: _WriterQueue | None) -> None:
        """Wait until a writer queue has matched every put with task_done."""
        if queue is None:
            return
        with contextlib.suppress(asyncio.CancelledError, Exception):
            await queue.join()

    async def _stop_feed_health_loop(self) -> None:
        """Cancel and await the feed-health flush loop if running."""
        task = self._feed_health_loop_task
        if task is None:
            return
        task.cancel()
        await self._await_shutdown_task(task)
        self._feed_health_loop_task = None

    async def _stop_tick_pipeline(self) -> None:
        """Stop and drain the tick consumer and writer pipeline."""
        if self._tick_consumer_task is not None:
            self._tick_consumer_task.cancel()
        await self._await_shutdown_task(self._tick_consumer_task)
        self._tick_consumer_task = None
        await self._join_shutdown_queue(self._tick_write_queue)
        await self._await_shutdown_task(self._tick_writer_task)
        self._tick_writer_task = None

    async def _stop_candle_pipeline(self) -> None:
        """Stop and drain candle consumers, the flush loops, and the writer pipeline.

        Ordering is load-bearing: both flush loops and the consumers are stopped
        before the native finalizer is drained (so no concurrent ``observe`` /
        ``flush`` mutates its held state), and the drain runs BEFORE the writer
        queue is joined so ``join`` blocks until every drained final bar is
        written — draining after the join would silently lose every held ended
        bar on a clean shutdown.
        """
        self._candle_shutdown_repair_drain_active = self._candle_writer_can_drain()
        if self._candle_flush_loop_task is not None:
            self._candle_flush_loop_task.cancel()
        await self._await_shutdown_task(self._candle_flush_loop_task)
        self._candle_flush_loop_task = None
        if self._candle_repair_loop_task is not None:
            self._candle_repair_loop_task.cancel()
        await self._await_shutdown_task(self._candle_repair_loop_task)
        self._candle_repair_loop_task = None
        if self._native_finalize_flush_task is not None:
            self._native_finalize_flush_task.cancel()
        await self._await_shutdown_task(self._native_finalize_flush_task)
        self._native_finalize_flush_task = None
        for task in self._candle_consumer_tasks:
            task.cancel()
        await self._await_shutdown_tasks(self._candle_consumer_tasks)
        self._candle_consumer_tasks = []
        if self._native_finalizer is not None:
            drained = self._native_finalizer.drain(datetime.now(UTC))
            if drained and self._candle_writer_can_drain():
                self._enqueue_finalized_candles(
                    drained,
                    self._get_data_exchange(),
                    self._get_exchange_name(),
                )
            elif drained:
                self._warn_abandoned_shutdown_candle_writes("native finalizer drain", len(drained))
        try:
            joined = await self._join_candle_shutdown_queue()
            if joined and self._candle_writer_can_drain():
                await self._drain_candle_repairs_on_shutdown(self._get_data_exchange())
                await self._join_candle_shutdown_queue()
        finally:
            self._candle_shutdown_repair_drain_active = False
        self._abandon_remaining_candle_repairs()
        await self._await_shutdown_task(self._candle_writer_task)
        self._candle_writer_task = None

    def _candle_writer_can_drain(self) -> bool:
        """Return whether the candle writer task can consume shutdown enqueues.

        Returns:
            True when a candle writer task exists and has not completed.
        """
        task = self._candle_writer_task
        return task is not None and not task.done()

    async def _join_candle_shutdown_queue(self) -> bool:
        """Join the candle writer queue without waiting behind a dead writer.

        Returns:
            True when the queue joined, False when the writer finished first.
        """
        task = self._candle_writer_task
        queue = self._candle_write_queue
        if queue is None:
            return True
        if task is None or task.done():
            if not queue.empty():
                self._warn_abandoned_shutdown_candle_writes("dead candle writer", queue.qsize())
            return False
        join_task = asyncio.create_task(queue.join())
        done, pending = await asyncio.wait({join_task, task}, return_when=asyncio.FIRST_COMPLETED)
        if join_task in done:
            await join_task
            return True
        for pending_task in pending:
            pending_task.cancel()
            await self._await_shutdown_task(pending_task)
        self._warn_abandoned_shutdown_candle_writes(
            "candle writer exited before queue join", queue.qsize()
        )
        return False

    def _warn_abandoned_shutdown_candle_writes(self, reason: str, row_count: int) -> None:
        """Log shutdown candle rows that cannot be consumed by the writer.

        Args:
            reason: Human-readable reason the rows cannot be drained.
            row_count: Number of rows affected.
        """
        logger.warning(
            f"abandoned candle shutdown writes: reason={reason} rows={row_count}; "
            "higher-timeframe rows resync on the next correction or manual backfill"
        )

    async def _stop_trade_pipeline(self) -> None:
        """Stop and drain the trade consumer and writer pipeline."""
        if self._trade_consumer_task is not None:
            self._trade_consumer_task.cancel()
        await self._await_shutdown_task(self._trade_consumer_task)
        self._trade_consumer_task = None
        await self._join_shutdown_queue(self._trade_write_queue)
        await self._await_shutdown_task(self._trade_writer_task)
        self._trade_writer_task = None

    async def _close_runtime_resources(self) -> None:
        """Disconnect exchange and release ZMQ resources."""
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

    def _supports_public_trades(self) -> bool:
        """Return whether this exchange provides a public trade feed.

        Override to False for exchanges that do not expose trade tape
        (e.g. Walutomat). When False, the trade loop is not started.

        Returns:
            True if the exchange supports public trade subscriptions.
        """
        return True

    async def _supervise_consumer(
        self,
        name: str,
        factory: Callable[[], Awaitable[None]],
    ) -> None:
        """Restart a consumer loop while the publisher is running."""
        while self.running:
            try:
                await factory()
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                if not self.running:
                    return
                logger.warning(
                    f"{self._get_exchange_name()}: consumer '{name}' raised {exc!r}; "
                    f"restarting in {_CONSUMER_RESTART_BACKOFF_S}s"
                )
            else:
                if not self.running:
                    return
                logger.warning(
                    f"{self._get_exchange_name()}: consumer '{name}' returned cleanly; "
                    f"restarting in {_CONSUMER_RESTART_BACKOFF_S}s"
                )
            await asyncio.sleep(_CONSUMER_RESTART_BACKOFF_S)

    def _get_liveness_recovery_threshold_s(self) -> int:
        """Return message-silence threshold before recovery is attempted."""
        return _LIVENESS_RECOVERY_THRESHOLD_S_DEFAULT

    def _candle_liveness_threshold_s(self) -> int:
        """Return native-candle silence threshold before recovery, or 0 to disable.

        The shared :attr:`_last_message_at` watchdog cannot see a dead native
        candle channel while ticks or trades keep flowing on the same socket
        (the 2026-06-18 kraken-spot ``ohlc:1m`` stall: trades alive ~21h, candles
        dark). Venues that receive 1m on a dedicated continuous native channel
        override this so a venue-wide candle silence triggers the same WS-restart
        recovery (which re-subscribes the dead channel). The base returns ``0``:
        trade-built / sparse venues already track candle liveness through trade
        liveness, so a separate guard would only false-fire.

        Returns:
            Seconds of venue-wide native-candle silence tolerated before a
            recovery is spawned, or ``0`` when the guard is disabled.
        """
        return 0

    def _spawn_recovery(self, reason: str, *, require_candle_progress: bool = False) -> None:
        """Schedule a tracked liveness recovery task with deduplication.

        Args:
            reason: Human-readable trigger reason for log context.
            require_candle_progress: When True (a candle-channel stall), the
                recovery only counts as successful once a fresh NATIVE candle
                arrives, observed over the longer candle grace — trades alone
                must not declare the dead candle channel recovered.
        """
        if monotonic() - self._last_recovery_at < _MIN_RECOVERY_INTERVAL_S:
            return
        if self._recovery_lock.locked():
            return
        self._last_recovery_at = monotonic()
        task = asyncio.create_task(
            self._run_recovery_under_lock(reason, require_candle_progress=require_candle_progress)
        )
        self._recovery_tasks.add(task)
        task.add_done_callback(self._recovery_tasks.discard)

    async def _run_recovery_under_lock(
        self, reason: str, *, require_candle_progress: bool = False
    ) -> None:
        """Persistently recover a stale feed under the recovery lock.

        Holds the publisher-level recovery lock for the whole recovery so
        only one recovery runs at a time (the heartbeat loop's repeated
        ``_spawn_recovery`` calls no-op while the lock is held). Each
        iteration rebuilds the venue connection via
        :meth:`_attempt_liveness_recovery`, then observes for fresh
        messages for up to ``_RECOVERY_PROGRESS_GRACE_S``. If data
        resumes the loop returns; otherwise it backs off with capped
        exponential jitter and tries again. The loop never abandons a
        dark feed on a timeout — it runs until messages resume or the
        publisher stops — so a multi-minute network outage no longer
        leaves the feed dark waiting for an operator. Each individual
        attempt is additionally bounded by ``_RECOVERY_ATTEMPT_TIMEOUT_S``
        so a wedged attempt (e.g. a replay stuck behind a blocked
        subscribe path) becomes a logged, retried failure instead of
        silently holding the recovery lock forever.

        Args:
            reason: Human-readable trigger reason for log context.
            require_candle_progress: When True, success additionally requires a
                fresh native candle (observed over the longer candle grace), so
                a candle-channel stall is not declared recovered by trade flow
                alone.

        Returns:
            None.

        Raises:
            asyncio.CancelledError: Propagated on shutdown so
                :meth:`stop` can join the recovery task cleanly.
        """
        async with self._recovery_lock:
            baseline = self._last_message_at
            candle_baseline = self._last_candle_msg_at if require_candle_progress else None
            backoff = _RECOVERY_BACKOFF_INITIAL_S
            attempt = 0
            while self.running:
                attempt += 1
                attempt_ok = True
                try:
                    async with asyncio.timeout(_RECOVERY_ATTEMPT_TIMEOUT_S):
                        await self._attempt_liveness_recovery(reason)
                except asyncio.CancelledError:
                    raise
                except Exception as exc:
                    attempt_ok = False
                    logger.warning(
                        f"{self._get_exchange_name()}: recovery attempt {attempt} "
                        f"raised {exc!r} (reason={reason}); will retry"
                    )
                if attempt_ok and await self._await_recovery_progress(baseline, candle_baseline):
                    logger.info(
                        f"{self._get_exchange_name()}: feed recovered after {attempt} "
                        f"attempt(s) (reason={reason})"
                    )
                    return
                await self._sleep_with_jitter(backoff)
                backoff = min(backoff * 2.0, _RECOVERY_BACKOFF_CAP_S)

    async def _await_recovery_progress(
        self, baseline: float, candle_baseline: float | None = None
    ) -> bool:
        """Observe for fresh messages after a recovery attempt.

        Args:
            baseline: ``_last_message_at`` captured when recovery started;
                progress means the feed advanced past this value.
            candle_baseline: When not None, ``_last_candle_msg_at`` captured at
                recovery start; success additionally requires a fresh native
                candle past it, observed over ``_CANDLE_RECOVERY_PROGRESS_GRACE_S``
                instead of the shorter message grace (the first candle only
                arrives at the next minute boundary).

        Returns:
            True as soon as the required watermarks advance past their
            baselines (data — and, when requested, a native candle — is
            flowing again), or False once the grace window elapses with no
            such progress.
        """
        grace = (
            _RECOVERY_PROGRESS_GRACE_S
            if candle_baseline is None
            else _CANDLE_RECOVERY_PROGRESS_GRACE_S
        )
        deadline = monotonic() + grace
        while self.running:
            if self._recovery_progress_made(baseline, candle_baseline):
                return True
            if monotonic() >= deadline:
                return False
            await asyncio.sleep(_RECOVERY_PROGRESS_POLL_S)
        return self._recovery_progress_made(baseline, candle_baseline)

    def _recovery_progress_made(self, baseline: float, candle_baseline: float | None) -> bool:
        """Return whether the watermarks required for recovery have advanced.

        Args:
            baseline: ``_last_message_at`` value captured at recovery start.
            candle_baseline: ``_last_candle_msg_at`` value captured at recovery
                start, or None when native-candle progress is not required.

        Returns:
            True when the message watermark advanced past ``baseline`` and,
            when ``candle_baseline`` is not None, the candle watermark advanced
            past it.
        """
        if self._last_message_at <= baseline:
            return False
        return candle_baseline is None or self._last_candle_msg_at > candle_baseline

    async def _sleep_with_jitter(self, seconds: float) -> None:
        """Sleep ``seconds`` with plus/minus ``_RECOVERY_JITTER_FRACTION`` jitter.

        Args:
            seconds: Base backoff duration before jitter is applied.

        Returns:
            None.
        """
        spread = seconds * _RECOVERY_JITTER_FRACTION
        jittered = seconds + random.uniform(-spread, spread)
        await asyncio.sleep(max(0.0, jittered))

    async def _attempt_liveness_recovery(self, reason: str) -> None:
        """Default liveness recovery hook used by publishers without recovery."""
        logger.error(
            f"{self._get_exchange_name()}: liveness exceeded ({reason}); "
            "no recovery handler configured"
        )
        await asyncio.sleep(0)

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

    def _native_candle_timeframes(self) -> frozenset[str]:
        """Timeframes this publisher can subscribe to natively from the venue.

        Used to filter the legacy (non-synthesizing) candle consumer so a
        1m-only venue never crash-loops calling ``subscribe_candles`` with an
        unsupported timeframe, and to flag (one-time) when synthesis REPLACES a
        venue's native higher-TF feed. Default ``{"1m"}`` covers the 1m-only
        venues (futures, equities, walutomat); kraken spot overrides with its
        full native OHLC set.

        Returns:
            The set of natively-subscribable timeframe labels.
        """
        return frozenset({"1m"})

    def _candle_stream_timeframes(self) -> frozenset[str]:
        """Timeframes this publisher can consume through the candle loop.

        Returns:
            Timeframes that should start a direct candle-loop subscription.
            The base implementation is exactly the native venue set.
        """
        return self._native_candle_timeframes()

    def _candle_source_for(self, timeframe: str) -> str:
        """Provenance tag for a native-path candle from this publisher.

        ``native`` means the bar is venue-precomputed upstream OHLC (Kraken
        spot's ``ohlc:1m``, Polygon grouped-daily history). Publishers that
        COMPUTE their 1m bars from the trade/quote stream (Kraken equities and
        futures via ``TradeCandleBuilder``, Walutomat via REST quote polling)
        override this to return ``calculated`` so provenance is unambiguous.
        Higher-TF rollups carry ``synthesized`` via :meth:`_publish_synthesized_candle`
        and never flow through here.

        Args:
            timeframe: The candle timeframe label (e.g. ``"1m"``).

        Returns:
            The ``source`` tag for the row (``native`` for upstream OHLC).
        """
        return "native"

    def _subscribe_candle_stream(
        self, symbols: list[str], timeframe: str
    ) -> AsyncIterator[CandleUpdate]:
        """Subscribe to the live candle stream for a timeframe.

        Args:
            symbols: Native symbols to subscribe for.
            timeframe: Candle timeframe interval.

        Returns:
            Async iterator of candle updates from the exchange client.
        """
        return cast(T, self._exchange_client).subscribe_candles(symbols, timeframe)

    def _candle_live_epoch(self) -> datetime:
        """The instant the aggregator should treat as the start of live data.

        A higher-TF window opening at or after this epoch is trustworthy (every
        minute observed). Live publishers use wall-clock now; the paper publisher
        overrides this to its REPLAY START so historical replayed windows are
        trustworthy and emitted (rather than suppressed as pre-epoch).

        Returns:
            The live-consumption start time (UTC).
        """
        return datetime.now(UTC)

    def _supports_forward_fill(self) -> bool:
        """Whether forward-fill (Phase 1b) is sound for this publisher's corpus.

        Forward-fill manufactures a flat bar for an empty higher-TF window; it is
        correct ONLY for a continuous-corpus venue (24/7 crypto), never for
        session-based equities, a 24/5 FX feed, or a historical replay. Default
        ``False`` — the global ``candle_forward_fill`` setting is gated by this so
        a wall-clock flush loop can never run for a venue (or paper) where it
        would manufacture bars the strategy was never validated on. Kraken spot
        overrides to ``True``.

        Returns:
            ``True`` if forward-fill may be enabled for this publisher.
        """
        return False

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
        repository = self._require_repository()
        now = datetime.now(UTC)
        symbol_pid = await resolve_symbol_public_id(repository, native_symbol, as_of=now)
        if symbol_pid is None:
            logger.warning(f"MarketDataPublisherService: No active Symbol row for {native_symbol}")
            return None
        _id, instrument_public_id = await repository.ensure_instrument(
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

    def _writer_loop_has_work[WriterRow: _WriterRow](
        self,
        queue: asyncio.Queue[WriterRow],
        state: _WriterBatchState[WriterRow],
    ) -> bool:
        """Return whether a writer loop still has rows to drain.

        Args:
            queue: Writer queue owned by the concrete data type.
            state: Mutable batch state for the current writer.

        Returns:
            True while the publisher is running, queued rows remain, or
            the current batch still needs flushing.
        """
        return (
            self.running
            or (queue is self._candle_write_queue and self._candle_shutdown_repair_drain_active)
            or not queue.empty()
            or bool(state.batch)
        )

    def _writer_shutdown_flush_needed[WriterRow: _WriterRow](
        self,
        queue: asyncio.Queue[WriterRow],
        state: _WriterBatchState[WriterRow],
    ) -> bool:
        """Return whether shutdown should flush the final held batch.

        Args:
            queue: Writer queue owned by the concrete data type.
            state: Mutable batch state for the current writer.

        Returns:
            True when the publisher is stopped, the queue is empty, and
            only the in-flight batch remains.
        """
        return not self.running and queue.empty() and bool(state.batch)

    def _writer_batch_age_elapsed[WriterRow: _WriterRow](
        self,
        state: _WriterBatchState[WriterRow],
        ev_loop: asyncio.AbstractEventLoop,
    ) -> bool:
        """Return whether the current writer batch should flush by age.

        Args:
            state: Mutable batch state for the current writer.
            ev_loop: Running event loop (for monotonic clock).

        Returns:
            True when there is no active timer or the timer has expired.
        """
        return (
            state.started_at is None or self._batch_age_remaining(state.started_at, ev_loop) <= 0.0
        )

    @staticmethod
    async def _flush_writer_state[WriterRow: _WriterRow](
        state: _WriterBatchState[WriterRow],
        flush_batch: _WriterFlush[WriterRow],
    ) -> None:
        """Flush a writer state batch and clear its timer after success.

        Args:
            state: Mutable batch state for the current writer.
            flush_batch: Concrete batch flush coroutine.
        """
        await flush_batch(state.batch)
        state.started_at = None

    async def _read_writer_row_or_flush_timeout[WriterRow: _WriterRow](
        self,
        queue: asyncio.Queue[WriterRow],
        state: _WriterBatchState[WriterRow],
        ev_loop: asyncio.AbstractEventLoop,
        poll_s: float,
        batch_age_budget_s: float,
        flush_batch: _WriterFlush[WriterRow],
    ) -> WriterRow | None:
        """Read one writer row or flush an aged batch after polling timeout.

        Args:
            queue: Writer queue owned by the concrete data type.
            state: Mutable batch state for the current writer.
            ev_loop: Running event loop (for monotonic clock).
            poll_s: Maximum wait per queue poll during shutdown.
            batch_age_budget_s: Remaining age budget for the current batch.
            flush_batch: Concrete batch flush coroutine.

        Returns:
            The next row when one was available, otherwise None.
        """
        try:
            async with asyncio.timeout(min(batch_age_budget_s, poll_s)):
                return await queue.get()
        except TimeoutError:
            if self._writer_batch_age_elapsed(state, ev_loop):
                await self._flush_writer_state(state, flush_batch)
            return None

    async def _append_writer_row_and_flush_if_full[WriterRow: _WriterRow](
        self,
        row: WriterRow,
        state: _WriterBatchState[WriterRow],
        ev_loop: asyncio.AbstractEventLoop,
        max_rows: int,
        flush_batch: _WriterFlush[WriterRow],
    ) -> None:
        """Append a writer row and flush the batch when it reaches capacity.

        Args:
            row: Row returned from the writer queue.
            state: Mutable batch state for the current writer.
            ev_loop: Running event loop (for monotonic clock).
            max_rows: Concrete writer batch size limit.
            flush_batch: Concrete batch flush coroutine.
        """
        state.batch.append(row)
        state.started_at = self._track_batch_start(state.started_at, ev_loop)
        if len(state.batch) >= max_rows:
            await self._flush_writer_state(state, flush_batch)

    async def _drain_writer_session[WriterRow: _WriterRow](
        self,
        queue: asyncio.Queue[WriterRow],
        state: _WriterBatchState[WriterRow],
        max_rows: int,
        poll_s: float,
        flush_batch: _WriterFlush[WriterRow],
    ) -> None:
        """Drain one writer session until shutdown or session loss.

        Args:
            queue: Writer queue owned by the concrete data type.
            state: Mutable batch state for the current writer.
            max_rows: Concrete writer batch size limit.
            poll_s: Maximum wait per queue poll during shutdown.
            flush_batch: Concrete batch flush coroutine.
        """
        ev_loop = asyncio.get_event_loop()
        while self._writer_loop_has_work(queue, state):
            if self._writer_shutdown_flush_needed(queue, state):
                await self._flush_writer_state(state, flush_batch)
                continue
            batch_age_budget_s = self._batch_age_remaining(state.started_at, ev_loop)
            if batch_age_budget_s <= 0.0:
                await self._flush_writer_state(state, flush_batch)
                continue
            row = await self._read_writer_row_or_flush_timeout(
                queue, state, ev_loop, poll_s, batch_age_budget_s, flush_batch
            )
            if row is None:
                continue
            await self._append_writer_row_and_flush_if_full(
                row, state, ev_loop, max_rows, flush_batch
            )

    def _writer_recovery_finished[WriterRow: _WriterRow](
        self,
        queue: asyncio.Queue[WriterRow],
        state: _WriterBatchState[WriterRow],
    ) -> bool:
        """Return whether writer recovery can stop after session loss.

        Args:
            queue: Writer queue owned by the concrete data type.
            state: Mutable batch state for the current writer.

        Returns:
            True when shutdown is complete and no rows remain.
        """
        return not self.running and queue.empty() and not state.batch

    @staticmethod
    async def _writer_recovery_backoff(
        label: str,
        backoff_s: float,
        held_rows: int,
        exc: _WriterSessionLostError,
    ) -> float:
        """Log writer session loss and sleep before reopening.

        Args:
            label: Data type label for log output.
            backoff_s: Current exponential backoff delay.
            held_rows: Rows still held in the writer batch.
            exc: Session loss exception that triggered recovery.

        Returns:
            The next capped exponential backoff delay.
        """
        logger.warning(
            f"{label} writer: session lost ({exc}); reopening in {backoff_s:.1f}s "
            f"with {held_rows} rows held"
        )
        await asyncio.sleep(backoff_s)
        return min(backoff_s * 2.0, _WRITER_RECONNECT_MAX_BACKOFF_S)

    async def _candle_loop(self, symbols: list[str], timeframe: str) -> None:
        """Subscribe to candle data, publish to ZMQ, and hand off DB rows to the writer.

        Ingest-only mirror of :meth:`_tick_loop`:
        drains the exchange WS iterator, publishes to ZMQ via
        :meth:`_process_candle`, and enqueues the resulting row on
        :attr:`_candle_write_queue` for the dedicated writer task.
        Never awaits ``_flush_candle_batch`` — that responsibility
        lives entirely in :meth:`_candle_writer_loop` so SQLite
        commit latency cannot block the WS-queue consumer.

        Uses the candle ID cache to ensure the same public_id is
        used on ZMQ and in the database for a given (instrument,
        timeframe, open_at) window.

        Each 1m frame is folded into the higher-timeframe aggregator
        SYNCHRONOUSLY, before the first ``await`` after receiving it, so the
        time-driven :meth:`_candle_flush_loop` (a separate task) cannot interleave
        at an intermediate await and finalize/seal a window before this frame is
        folded — which would otherwise drop the in-hand frame as late. The
        synthesized bars are published after the native 1m to preserve outbound
        ordering.

        Args:
            symbols: List of symbols to subscribe to for candle data.
            timeframe: Candle timeframe interval (e.g., '1m', '5m', '1h').
        """
        if not self._exchange_client:
            logger.error(_EXCHANGE_NOT_INIT_MSG)
            return
        exchange = self._get_data_exchange()
        exchange_label = self._get_exchange_name()
        iterator = self._subscribe_candle_stream(symbols, timeframe).__aiter__()
        next_fut: asyncio.Future[CandleUpdate | object] | None = None
        try:
            while self.running:
                next_fut = next_fut or asyncio.ensure_future(anext(iterator, _STREAM_END))
                done, _ = await asyncio.wait({next_fut}, timeout=self._batch_max_age_s)
                if not done:
                    continue
                next_fut = None
                candle = done.pop().result()
                should_continue = await self._handle_candle_stream_item(
                    candle, exchange, exchange_label, timeframe
                )
                if not should_continue:
                    break
        except asyncio.CancelledError:
            raise
        except Exception as e:
            logger.error(f"Candle loop error for {symbols}: {e}")
        finally:
            _cleanup_pending_future(next_fut)

    async def _handle_candle_stream_item(
        self,
        candle: CandleUpdate | object,
        exchange: MarketDataExchange,
        exchange_label: str,
        timeframe: str,
    ) -> bool:
        """Given one stream item, when consumed, then publish and enqueue its effects.

        Args:
            candle: Stream item returned by the candle iterator.
            exchange: Data exchange stamped on outbound/persisted rows.
            exchange_label: Exchange label used for write-queue drop logging.
            timeframe: Native timeframe of the stream being consumed.

        Returns:
            ``False`` when the iterator sentinel was observed, otherwise ``True``.

        Raises:
            Exception: Propagates publish, process, or synthesis failures unchanged.
        """
        if candle is _STREAM_END:
            return False
        candle_update = cast(CandleUpdate, candle)
        synthesized: list[tuple[str, CandleUpdate]] = []
        aggregator = self._candle_aggregator
        if aggregator is not None and timeframe == "1m":
            synthesized = aggregator.fold(candle_update)
            self._drain_aggregator_repair_signals()
        row = await self._process_candle(candle_update, exchange, timeframe)
        if row is not None:
            released = self._observe_native_candle(candle_update.symbol, row)
            self._enqueue_finalized_candles(released, exchange, exchange_label)
        for tf_label, synth in synthesized:
            await self._publish_synthesized_candle(synth, exchange, tf_label)
        return True

    def _observe_native_candle(
        self, native_symbol: str, row: CandleUpsertRow
    ) -> list[tuple[str, CandleUpsertRow]]:
        """Route a native candle row through the finalizer (or pass it through).

        Args:
            native_symbol: Native symbol of the frame (for the persist gate).
            row: The built candle row for the current frame.

        Returns:
            ``(native_symbol, row)`` pairs to persist — the finalizer's release
            set, or the row as-is when no finalizer is installed (non-candle
            publishers / direct-call tests).
        """
        if self._native_finalizer is None:
            return [(native_symbol, row)]
        return self._native_finalizer.observe(native_symbol, row)

    def _enqueue_finalized_candles(
        self,
        released: list[tuple[str, CandleUpsertRow]],
        exchange: MarketDataExchange,
        exchange_label: str,
    ) -> list[_EnqueuedCandleWrite]:
        """Enqueue each released candle row that passes the per-symbol persist gate.

        Args:
            released: ``(native_symbol, row)`` pairs released by the finalizer.
            exchange: Exchange for the persist-policy lookup.
            exchange_label: Exchange label for write-queue drop logging.

        Returns:
            Enqueued rows paired with their write sequence.
        """
        enqueued: list[_EnqueuedCandleWrite] = []
        for native_symbol, row in released:
            if self._should_persist_row("candles", exchange, native_symbol):
                sequence = self._assign_candle_write_sequence(row)
                evicted = _enqueue_or_drop_oldest_candle_write(
                    self._candle_write_queue, row, exchange_label
                )
                if evicted is not None:
                    self._forget_candle_write_sequence(evicted)
                enqueued.append(_EnqueuedCandleWrite(native_symbol, row, sequence))
        self._bind_enqueued_corrections_to_late_drops(enqueued)
        return enqueued

    def _assign_candle_write_sequence(self, row: CandleUpsertRow) -> int:
        """Assign and remember the monotonic write sequence for a queued row.

        Args:
            row: Candle row placed on the writer queue.

        Returns:
            Monotonic publisher-local write sequence.
        """
        self._next_candle_write_sequence += 1
        sequence = self._next_candle_write_sequence
        self._candle_write_sequence_by_row_id[id(row)] = sequence
        return sequence

    def _forget_candle_write_sequence(self, row: CandleUpsertRow) -> int | None:
        """Drop tracking for a row that will not be committed by the writer.

        Args:
            row: Candle row leaving the queue without a successful commit.

        Returns:
            The forgotten sequence if the row was tracked.
        """
        sequence = self._pop_candle_write_sequence(row)
        if sequence is not None:
            self._drop_uncommitted_candle_write_dependents(sequence)
        return sequence

    def _pop_candle_write_sequence(self, row: CandleUpsertRow) -> int | None:
        """Drop row-to-sequence tracking without changing repair state.

        Args:
            row: Candle row whose tracking should be removed.

        Returns:
            The forgotten sequence if the row was tracked.
        """
        return self._candle_write_sequence_by_row_id.pop(id(row), None)

    def _mark_candle_writes_committed(self, rows: list[CandleUpsertRow]) -> None:
        """Mark queued candle rows as durably committed by exact sequence.

        Args:
            rows: Candle rows successfully committed by the writer.
        """
        committed = False
        for row in rows:
            sequence = self._pop_candle_write_sequence(row)
            if sequence is None:
                continue
            self._committed_candle_write_sequences.add(sequence)
            self._cleanup_committed_candle_write_sequence(sequence)
            committed = True
        if committed:
            self._candle_repair_event.set()

    def _forget_candle_writes(self, rows: list[CandleUpsertRow]) -> None:
        """Forget queued rows that the writer dropped without a commit.

        Args:
            rows: Candle rows that will not be retried.
        """
        for row in rows:
            self._forget_candle_write_sequence(row)

    def _drop_uncommitted_candle_write_dependents(self, sequence: int) -> None:
        """Remove repair gates that waited on an uncommitted candle write.

        Args:
            sequence: Forgotten write sequence that did not commit.
        """
        late_keys = {
            late_key
            for late_key, pending in self._pending_late_candle_drops.items()
            if pending.write_sequence == sequence
        }
        repair_keys = {
            key
            for key, pending in self._pending_candle_repairs.items()
            if pending.required_sequence == sequence
        }
        for late_key in late_keys:
            repair_keys.update(self._pending_late_candle_drops[late_key].repair_keys)
        if not late_keys and not repair_keys:
            return
        for repair_key in repair_keys:
            self._pending_candle_repairs.pop(repair_key, None)
            self._repair_drop_key_by_repair_key.pop(repair_key, None)
        for late_key in late_keys:
            self._pending_late_candle_drops.pop(late_key, None)
        for pending in self._pending_late_candle_drops.values():
            pending.repair_keys.difference_update(repair_keys)
        self._committed_candle_write_sequences.discard(sequence)
        logger.warning(
            f"abandoned candle repairs for uncommitted correction write: "
            f"sequence={sequence} repairs={len(repair_keys)} late_drops={len(late_keys)}; "
            "higher-timeframe rows resync on the next correction or manual backfill"
        )

    async def _native_finalize_flush_loop(self, exchange: MarketDataExchange) -> None:
        """Release native bars whose window has ended (illiquid/stalled symbols).

        Mirrors :meth:`_candle_flush_loop`'s scaffolding but for the native-1m
        finalizer: every :data:`_CANDLE_FLUSH_INTERVAL_S` it asks the finalizer to
        release any held window ended past the grace and enqueues each for
        persistence. Runs for every native-candle publisher (not only
        forward-fill venues) so a symbol that stops trading still gets its last
        bar persisted. One bad tick is logged and never kills the loop.

        Args:
            exchange: Exchange name for provenance / persist-policy lookup.
        """
        exchange_label = self._get_exchange_name()
        while self.running:
            try:
                await asyncio.sleep(_CANDLE_FLUSH_INTERVAL_S)
                if not self.running:
                    break
                if self._native_finalizer is None:
                    continue
                self._enqueue_finalized_candles(
                    self._native_finalizer.flush(datetime.now(UTC)), exchange, exchange_label
                )
            except asyncio.CancelledError:
                raise
            except Exception as e:
                logger.error(f"Native candle finalize flush loop error: {e}")

    async def _candle_flush_loop(self, exchange: MarketDataExchange) -> None:
        """Drive the time-driven higher-TF candle flush (Phase 1b forward-fill).

        Started only when ``candle_forward_fill`` is enabled and higher
        timeframes are synthesized. Every :data:`_CANDLE_FLUSH_INTERVAL_S` it asks
        the aggregator to seal any wall-clock-ended higher-TF window and
        forward-fill empty windows, then publishes each resulting bar exactly like
        the live fold path (publish-only, no persist). One bad tick is logged and
        never kills the loop, mirroring :meth:`_heartbeat_loop`.

        Args:
            exchange: Exchange name for message provenance.
        """
        while self.running:
            try:
                await asyncio.sleep(_CANDLE_FLUSH_INTERVAL_S)
                if not self.running:
                    break
                if self._candle_aggregator is None:
                    continue
                flushed = self._candle_aggregator.flush(datetime.now(UTC))
                self._drain_aggregator_repair_signals()
                for tf_label, synth in flushed:
                    await self._publish_synthesized_candle(synth, exchange, tf_label)
            except asyncio.CancelledError:
                raise
            except Exception as e:
                logger.error(f"Candle flush loop error: {e}")

    def _pop_aggregator_late_drops(self) -> list[LateCandleDrop]:
        """Drain late-drop signals from the configured real aggregator.

        Returns:
            Late-drop signals, or an empty list when synthesis is not active.
        """
        aggregator = self._candle_aggregator
        if isinstance(aggregator, CandleAggregator):
            return aggregator.pop_late_drops()
        return []

    def _pop_aggregator_seeded_incomplete_windows(self) -> list[SeededIncompleteWindow]:
        """Drain seeded-incomplete close signals from the configured aggregator.

        Returns:
            Suppressed seeded windows that should be count-gated against the
            persisted 1m plane.
        """
        aggregator = self._candle_aggregator
        if isinstance(aggregator, CandleAggregator):
            return aggregator.pop_seeded_incomplete_windows()
        return []

    def _drain_aggregator_repair_signals(self) -> None:
        """Drain pure aggregator repair signals into publisher repair queues."""
        self._record_late_candle_drops(self._pop_aggregator_late_drops())
        self._record_seeded_incomplete_window_repairs(
            self._pop_aggregator_seeded_incomplete_windows()
        )

    def _record_late_candle_drops(self, drops: list[LateCandleDrop]) -> None:
        """Remember late drops until a matching correction write commits.

        Args:
            drops: Pure late-drop signals drained from the aggregator.
        """
        for drop in drops:
            key = _LateCandleDropKey(drop.symbol, drop.minute)
            if key in self._pending_late_candle_drops:
                continue
            if len(self._pending_late_candle_drops) >= _CANDLE_REPAIR_PENDING_MAX:
                dropped = next(iter(self._pending_late_candle_drops))
                self._drop_late_candle_drop(dropped)
                logger.error(
                    f"late candle repair queue exceeded {_CANDLE_REPAIR_PENDING_MAX}; "
                    f"dropped oldest late signal symbol={dropped.symbol} "
                    f"minute={dropped.minute.isoformat()}"
                )
            self._pending_late_candle_drops[key] = _PendingLateCandleDrop()

    def _record_seeded_incomplete_window_repairs(
        self, windows: list[SeededIncompleteWindow]
    ) -> None:
        """Schedule source-gated repairs for suppressed seeded windows.

        Args:
            windows: Pure seeded incomplete close signals drained from the
                aggregator.
        """
        for window in windows:
            key = _CandleRepairKey(window.symbol, window.timeframe, window.window_begin)
            self._schedule_seeded_incomplete_candle_repair_key(key, window.counted_minutes)

    def _bind_enqueued_corrections_to_late_drops(
        self, enqueued: list[_EnqueuedCandleWrite]
    ) -> None:
        """Bind complete 1m writes to matching late-drop repair work.

        Args:
            enqueued: Candle writes accepted by the writer queue.
        """
        for item in enqueued:
            row = item.row
            if row["timeframe"] != "1m" or not row["complete"]:
                continue
            late_key = _LateCandleDropKey(item.native_symbol, row["open_at"])
            pending = self._pending_late_candle_drops.get(late_key)
            if pending is None:
                continue
            pending.write_sequence = item.sequence
            self._schedule_late_candle_repairs(late_key, item.sequence)

    def _schedule_late_candle_repairs(
        self, late_key: _LateCandleDropKey, required_sequence: int
    ) -> None:
        """Schedule higher-timeframe repairs for one late drop and correction.

        Args:
            late_key: Late 1m drop key.
            required_sequence: Exact correction write sequence that must commit.
        """
        aggregator = self._candle_aggregator
        pending_drop = self._pending_late_candle_drops.get(late_key)
        if aggregator is None or pending_drop is None:
            return
        for timeframe in aggregator.timeframes:
            begin = CandleAggregator._floor(
                late_key.minute, aggregator.timeframe_seconds(timeframe)
            )
            repair_key = _CandleRepairKey(late_key.symbol, timeframe, begin)
            self._schedule_candle_repair_key(repair_key, required_sequence, late_key)

    def _schedule_seeded_incomplete_candle_repair_key(
        self, key: _CandleRepairKey, counted_minutes: frozenset[datetime]
    ) -> None:
        """Add or refresh one source-gated seeded-incomplete repair key.

        Args:
            key: Synthesized candle natural key to repair.
            counted_minutes: Distinct complete 1m minutes folded live or from
                the seed before the bucket was suppressed.
        """
        pending = self._pending_candle_repairs.get(key)
        if pending is not None:
            previous_minutes = pending.expected_source_minutes
            if previous_minutes is None:
                pending.expected_source_minutes = counted_minutes
                pending.expected_minute_count = len(counted_minutes)
                pending.last_observed_minute_count = None
                pending.attempts = 0
            else:
                merged_minutes = previous_minutes | counted_minutes
                if merged_minutes != previous_minutes:
                    pending.expected_source_minutes = frozenset(merged_minutes)
                    pending.expected_minute_count = len(merged_minutes)
                    pending.last_observed_minute_count = None
                    pending.attempts = 0
            self._candle_repair_event.set()
            return
        if len(self._pending_candle_repairs) >= _CANDLE_REPAIR_PENDING_MAX:
            dropped = next(iter(self._pending_candle_repairs))
            self._drop_pending_candle_repair(dropped)
            logger.error(
                f"candle repair queue exceeded {_CANDLE_REPAIR_PENDING_MAX}; "
                f"dropped oldest key symbol={dropped.symbol} "
                f"timeframe={dropped.timeframe} window={dropped.window_begin.isoformat()}"
            )
        self._pending_candle_repairs[key] = _PendingCandleRepair(
            expected_minute_count=len(counted_minutes),
            expected_source_minutes=counted_minutes,
        )
        self._candle_repair_event.set()

    def _schedule_candle_repair_key(
        self, key: _CandleRepairKey, required_sequence: int, late_key: _LateCandleDropKey
    ) -> None:
        """Add or refresh one debounced synthesized-candle repair key.

        Args:
            key: Synthesized candle natural key to repair.
            required_sequence: Exact correction write sequence that must commit.
            late_key: Late 1m drop this repair came from.
        """
        pending = self._pending_candle_repairs.get(key)
        if pending is not None:
            if pending.required_sequence is None or pending.required_sequence < required_sequence:
                old_sequence = pending.required_sequence
                self._unlink_repair_from_late_drop(key)
                pending.required_sequence = required_sequence
                pending.attempts = 0
                self._link_repair_to_late_drop(key, late_key)
                if old_sequence is not None:
                    self._cleanup_committed_candle_write_sequence(old_sequence)
            self._candle_repair_event.set()
            return
        if len(self._pending_candle_repairs) >= _CANDLE_REPAIR_PENDING_MAX:
            dropped = next(iter(self._pending_candle_repairs))
            self._drop_pending_candle_repair(dropped)
            logger.error(
                f"candle repair queue exceeded {_CANDLE_REPAIR_PENDING_MAX}; "
                f"dropped oldest key symbol={dropped.symbol} "
                f"timeframe={dropped.timeframe} window={dropped.window_begin.isoformat()}"
            )
        self._pending_candle_repairs[key] = _PendingCandleRepair(required_sequence)
        self._link_repair_to_late_drop(key, late_key)
        self._candle_repair_event.set()

    def _link_repair_to_late_drop(
        self, key: _CandleRepairKey, late_key: _LateCandleDropKey
    ) -> None:
        """Record which late drop owns a repair key.

        Args:
            key: Repair key.
            late_key: Late-drop key.
        """
        self._repair_drop_key_by_repair_key[key] = late_key
        pending_drop = self._pending_late_candle_drops.get(late_key)
        if pending_drop is not None:
            pending_drop.repair_keys.add(key)

    def _unlink_repair_from_late_drop(self, key: _CandleRepairKey) -> None:
        """Remove a repair key from its owning late-drop record.

        Args:
            key: Repair key to unlink.
        """
        late_key = self._repair_drop_key_by_repair_key.pop(key, None)
        if late_key is None:
            return
        pending_drop = self._pending_late_candle_drops.get(late_key)
        if pending_drop is None:
            return
        pending_drop.repair_keys.discard(key)
        if not pending_drop.repair_keys and pending_drop.write_sequence is not None:
            del self._pending_late_candle_drops[late_key]

    def _drop_pending_candle_repair(self, key: _CandleRepairKey) -> None:
        """Drop one pending repair and unlink its late-drop owner.

        Args:
            key: Repair key to remove.
        """
        sequences = self._sequences_for_repair_key(key)
        self._pending_candle_repairs.pop(key, None)
        self._unlink_repair_from_late_drop(key)
        for sequence in sequences:
            self._cleanup_committed_candle_write_sequence(sequence)

    def _drop_late_candle_drop(self, late_key: _LateCandleDropKey) -> None:
        """Drop one pending late signal and all repair keys it owns.

        Args:
            late_key: Late-drop key to remove.
        """
        pending_drop = self._pending_late_candle_drops.pop(late_key, None)
        if pending_drop is None:
            return
        sequences: set[int] = set()
        if pending_drop.write_sequence is not None:
            sequences.add(pending_drop.write_sequence)
        for repair_key in tuple(pending_drop.repair_keys):
            pending_repair = self._pending_candle_repairs.get(repair_key)
            if pending_repair is not None and pending_repair.required_sequence is not None:
                sequences.add(pending_repair.required_sequence)
            self._pending_candle_repairs.pop(repair_key, None)
            self._repair_drop_key_by_repair_key.pop(repair_key, None)
        for sequence in sequences:
            self._cleanup_committed_candle_write_sequence(sequence)

    def _sequences_for_repair_key(self, key: _CandleRepairKey) -> set[int]:
        """Collect committed-ack candidates referenced by one repair key.

        Args:
            key: Repair key whose sequence references should be collected.

        Returns:
            Sequence values that may become unreferenced after the key is dropped.
        """
        sequences: set[int] = set()
        pending_repair = self._pending_candle_repairs.get(key)
        if pending_repair is not None and pending_repair.required_sequence is not None:
            sequences.add(pending_repair.required_sequence)
        late_key = self._repair_drop_key_by_repair_key.get(key)
        if late_key is None:
            return sequences
        pending_drop = self._pending_late_candle_drops.get(late_key)
        if pending_drop is not None and pending_drop.write_sequence is not None:
            sequences.add(pending_drop.write_sequence)
        return sequences

    def _cleanup_committed_candle_write_sequence(self, sequence: int | None) -> None:
        """Forget an exact commit ack once no pending repair needs it.

        Args:
            sequence: Candle write sequence to consider for cleanup, or
                ``None`` for count-only repairs.
        """
        if sequence is None:
            return
        if any(
            pending.required_sequence == sequence
            for pending in self._pending_candle_repairs.values()
        ):
            return
        if any(
            pending.write_sequence == sequence
            for pending in self._pending_late_candle_drops.values()
        ):
            return
        self._committed_candle_write_sequences.discard(sequence)

    async def _candle_repair_loop(self, exchange: MarketDataExchange) -> None:
        """Run debounced durable re-derivation for late 1m corrections.

        Args:
            exchange: Exchange name for synthesized repair publication.
        """
        while self.running:
            try:
                await self._candle_repair_event.wait()
                self._candle_repair_event.clear()
                await asyncio.sleep(_CANDLE_REPAIR_DEBOUNCE_S)
                if not self.running:
                    break
                await self._run_due_candle_repairs(exchange)
            except asyncio.CancelledError:
                raise
            except Exception as e:
                logger.error(f"Candle repair loop error: {e}")

    async def _run_due_candle_repairs(self, exchange: MarketDataExchange) -> None:
        """Repair every pending key whose source commit and frontier are ready.

        Args:
            exchange: Exchange name for synthesized repair publication.
        """
        groups = self._due_candle_repair_groups()
        if not groups:
            return
        semaphore = asyncio.Semaphore(_CANDLE_REPAIR_MAX_CONCURRENCY)
        tasks = [
            asyncio.create_task(self._repair_candle_group(entries, exchange, semaphore))
            for entries in groups.values()
        ]
        results_by_group = await asyncio.gather(*tasks)
        retry = False
        for results in results_by_group:
            for result in results:
                pending = self._pending_candle_repairs.get(result.key)
                if result.done:
                    if (
                        pending is not None
                        and pending.required_sequence == result.required_sequence
                    ):
                        self._drop_pending_candle_repair(result.key)
                        self._cleanup_committed_candle_write_sequence(result.required_sequence)
                else:
                    retry = (
                        self._record_failed_candle_repair_attempt(
                            result.key, result.required_sequence
                        )
                        or retry
                    )
        if retry:
            self._candle_repair_event.set()

    def _record_failed_candle_repair_attempt(
        self, key: _CandleRepairKey, required_sequence: int | None
    ) -> bool:
        """Record a failed repair attempt and decide whether it may retry.

        Args:
            key: Repair key that failed to produce a candle.
            required_sequence: Correction sequence used by the failed attempt,
                or ``None`` for count-only seeded repairs.

        Returns:
            True when the repair should be retried, False when it was dropped or
            no longer matches the pending state.
        """
        pending = self._pending_candle_repairs.get(key)
        if pending is None or pending.required_sequence != required_sequence:
            return False
        pending.attempts += 1
        max_attempts = (
            _SEEDED_INCOMPLETE_REPAIR_MAX_ATTEMPTS
            if pending.expected_source_minutes is not None
            else _CANDLE_REPAIR_MAX_ATTEMPTS
        )
        if pending.attempts < max_attempts:
            return True
        self._drop_pending_candle_repair(key)
        self._cleanup_committed_candle_write_sequence(required_sequence)
        if pending.expected_source_minutes is not None:
            observed = (
                pending.last_observed_minute_count
                if pending.last_observed_minute_count is not None
                else 0
            )
            logger.warning(
                f"abandoned seeded incomplete candle repair after source-set gate: "
                f"symbol={key.symbol} timeframe={key.timeframe} "
                f"window={key.window_begin.isoformat()} "
                f"source_1m_count={observed}/{pending.expected_minute_count} "
                f"attempts={pending.attempts}; manual backfill required"
            )
            return False
        logger.warning(
            f"abandoned candle repair after retry limit: symbol={key.symbol} "
            f"timeframe={key.timeframe} window={key.window_begin.isoformat()} "
            f"attempts={pending.attempts}; source 1m remains incomplete or unavailable; "
            "manual backfill resyncs the synthesized window"
        )
        return False

    async def _drain_candle_repairs_on_shutdown(self, exchange: MarketDataExchange) -> None:
        """Drain committed and closed candle repairs during graceful shutdown.

        Args:
            exchange: Exchange name for synthesized repair publication.
        """
        if not self._pending_candle_repairs:
            return
        await self._run_due_candle_repairs(exchange)

    def _abandon_remaining_candle_repairs(self) -> None:
        """Log and clear repair state that cannot be completed on shutdown."""
        repair_count = len(self._pending_candle_repairs)
        late_count = len(self._pending_late_candle_drops)
        if repair_count == 0 and late_count == 0:
            return
        logger.warning(
            f"abandoned pending candle repairs during shutdown: repairs={repair_count} "
            f"late_drops={late_count}; committed 1m plane remains the source of truth "
            "and higher-timeframe rows resync on the next correction or manual backfill"
        )
        self._pending_candle_repairs.clear()
        self._pending_late_candle_drops.clear()
        self._repair_drop_key_by_repair_key.clear()
        self._committed_candle_write_sequences.clear()
        self._candle_write_sequence_by_row_id.clear()

    def _due_candle_repair_groups(
        self,
    ) -> dict[
        tuple[str, datetime], list[tuple[_CandleRepairKey, int | None, frozenset[datetime] | None]]
    ]:
        """Collect repair keys whose writer sequence and frontier are ready.

        Returns:
            Pending repair entries grouped by native symbol and window start.
        """
        aggregator = self._candle_aggregator
        if aggregator is None:
            return {}
        groups: dict[
            tuple[str, datetime],
            list[tuple[_CandleRepairKey, int | None, frozenset[datetime] | None]],
        ] = {}
        for key, pending in self._pending_candle_repairs.items():
            if (
                pending.required_sequence is not None
                and pending.required_sequence not in self._committed_candle_write_sequences
            ):
                continue
            if not aggregator.has_closed_window(key.symbol, key.timeframe, key.window_begin):
                continue
            group_key = (key.symbol, key.window_begin)
            groups.setdefault(group_key, []).append(
                (key, pending.required_sequence, pending.expected_source_minutes)
            )
        return groups

    async def _repair_candle_group(
        self,
        entries: list[tuple[_CandleRepairKey, int | None, frozenset[datetime] | None]],
        exchange: MarketDataExchange,
        semaphore: asyncio.Semaphore,
    ) -> list[_CandleRepairResult]:
        """Repair a group of keys under the shared concurrency cap.

        Args:
            entries: Pending repair keys, required sequences, and count gates.
            exchange: Exchange name for synthesized repair publication.
            semaphore: Shared concurrency limiter.

        Returns:
            Repair results for each entry.
        """
        async with semaphore:
            results: list[_CandleRepairResult] = []
            for key, required_sequence, expected_source_minutes in entries:
                done = await self._repair_candle_key(key, exchange, expected_source_minutes)
                results.append(_CandleRepairResult(key, required_sequence, done))
            return results

    async def _repair_candle_key(
        self,
        key: _CandleRepairKey,
        exchange: MarketDataExchange,
        expected_source_minutes: frozenset[datetime] | None = None,
    ) -> bool:
        """Re-derive and publish one synthesized candle from persisted 1m rows.

        Args:
            key: Synthesized candle natural key to repair.
            exchange: Exchange name for synthesized repair publication.
            expected_source_minutes: Source-set gate for seeded-incomplete repairs, or
                ``None`` for exact late-correction repairs.

        Returns:
            True when the pending key should be removed, False when it should be
            retried after another debounce interval.
        """
        try:
            candle = await self._derive_repaired_synthesized_candle(key, expected_source_minutes)
            if candle is None:
                return False
            await self._publish_synthesized_candle(candle, exchange, key.timeframe)
            return True
        except Exception as e:
            logger.error(
                f"candle repair failed: symbol={key.symbol} timeframe={key.timeframe} "
                f"window={key.window_begin.isoformat()} error={e}"
            )
            return False

    async def _derive_repaired_synthesized_candle(
        self, key: _CandleRepairKey, expected_source_minutes: frozenset[datetime] | None = None
    ) -> CandleUpdate | None:
        """Read persisted 1m rows and rebuild one higher-timeframe candle.

        Args:
            key: Synthesized candle natural key to repair.
            expected_source_minutes: Distinct complete 1m source minutes that
                must be present for a seeded-incomplete repair, or ``None`` for
                exact repairs.

        Returns:
            Re-derived synthesized candle, or ``None`` when fail-safe guards
            suppress the repair.
        """
        repository = self.repository
        aggregator = self._candle_aggregator
        if repository is None or aggregator is None:
            return None
        tf_seconds = aggregator.timeframe_seconds(key.timeframe)
        window_end = key.window_begin + timedelta(seconds=tf_seconds)
        rows = await repository.get_candles(
            key.symbol,
            "1m",
            key.window_begin,
            window_end - timedelta(microseconds=1),
            self._get_exchange_name(),
            datetime.now(UTC),
            order="asc",
        )
        pending = self._pending_candle_repairs.get(key)
        observed_source_minutes = {row["open_at"] for row in rows if row["complete"]}
        if pending is not None and expected_source_minutes is not None:
            pending.last_observed_minute_count = len(observed_source_minutes)
        if not rows:
            return None
        if any(not row["complete"] for row in rows):
            return None
        if expected_source_minutes is not None and not observed_source_minutes.issuperset(
            expected_source_minutes
        ):
            return None
        repair_aggregator = CandleAggregator(
            [key.timeframe],
            live_epoch=key.window_begin,
            forward_fill=True,
        )
        for row in rows:
            repair_aggregator.fold(self._candle_update_from_row(row, key.symbol))
        for timeframe, candle in repair_aggregator.flush(window_end):
            if timeframe == key.timeframe and candle.interval_begin == key.window_begin:
                return candle
        return None

    async def _candle_writer_loop(self) -> None:
        """Drain :attr:`_candle_write_queue` and flush candles to the database.

        Mirror of :meth:`_tick_writer_loop` — see that docstring for the
        drain / ``task_done`` / ``timeout <= 0`` invariants. Candle
        volume is ~10-50× lower than ticks (one row per
        ``timeframe`` window per instrument), so the queue cap is
        ``_CANDLE_WRITE_QUEUE_MAX``.
        """
        state = _WriterBatchState[CandleUpsertRow]([])
        backoff_s = _WRITER_RECONNECT_INITIAL_BACKOFF_S
        while True:
            try:
                async with self._open_candle_writer_session() as writer_session:
                    self._candle_writer_session = writer_session
                    try:
                        await self._drain_writer_session(
                            self._candle_write_queue,
                            state,
                            self._candle_batch_max_rows,
                            _CANDLE_WRITER_SHUTDOWN_POLL_S,
                            self._flush_candle_writer_batch,
                        )
                    finally:
                        self._candle_writer_session = None
                return
            except _WriterSessionLostError as exc:
                if self._writer_recovery_finished(self._candle_write_queue, state):
                    return
                backoff_s = await self._writer_recovery_backoff(
                    "candle", backoff_s, len(state.batch), exc
                )

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

        On the :class:`_WriterSessionLostError` path the row-by-row
        fallback may have already trimmed committed rows off ``batch``;
        we acknowledge the trimmed prefix (``flushed_count -
        len(batch)``) with ``task_done`` so ``queue.join`` stays
        accurate, then re-raise so the writer loop can dispose the
        dead session and re-open.

        Args:
            batch: In-flight write batch. Cleared in place on a
                fully-successful flush; left at "retry-pending" length
                when a disconnect mid-flush triggers re-acquisition.
        """
        if not batch:
            return
        flushed_count = len(batch)
        try:
            await self._flush_candle_batch(batch)
        except _WriterSessionLostError:
            committed_prefix = flushed_count - len(batch)
            for _ in range(committed_prefix):
                self._candle_write_queue.task_done()
            raise
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
        self._mark_candle_liveness_progress()
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
        window_closed = received_at >= candle.interval_begin + timedelta(
            seconds=window_seconds(timeframe)
        )
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
            complete=window_closed,
        )
        row = self._build_candle_row(
            candle_msg,
            instrument_public_id,
            source=self._candle_source_for(timeframe),
            complete=candle_msg.complete,
        )
        await self._publish_message(topic, candle_msg)
        self._last_data_timestamps[native_symbol] = received_at.timestamp() * 1000
        return row

    def _mark_candle_liveness_progress(self) -> None:
        """Record live candle progress for feed recovery checks.

        Returns:
            None.
        """
        now = monotonic()
        self._last_message_at = now
        self._last_candle_msg_at = now

    async def _publish_synthesized_candle(
        self, candle: CandleUpdate, exchange: MarketDataExchange, timeframe: str
    ) -> None:
        """Publish AND persist a synthesized higher-timeframe candle (Phase 3).

        The persisting twin of :meth:`_process_candle` for bars the
        :class:`CandleAggregator` rolls up from the 1m stream. Resolves the
        instrument, reuses the stable candle ``public_id`` (so re-emissions are
        idempotent), publishes to ZMQ, and — gated by the SAME persist policy as
        the native 1m path (:meth:`_should_persist_row`) — enqueues a durable
        row tagged ``source='synthesized'`` carrying the aggregator's
        trustworthy-boundary ``complete`` flag. A re-emitted ``(instrument,
        timeframe, open_at)`` is an SCD2 no-op unless OHLCV or provenance
        changed (e.g. a completeness flip). Publish is unconditional; only the
        DB write is policy-gated, mirroring the native path.

        Args:
            candle: Synthesized higher-timeframe candle from the aggregator.
            exchange: Exchange name for message provenance.
            timeframe: Explicit timeframe label from the aggregator (never
                decoded from ``candle.interval``).
        """
        native_symbol = candle.symbol
        instrument_public_id = await self._ensure_instrument(native_symbol)
        if instrument_public_id is None:
            return
        public_id = self._resolve_candle_public_id(
            instrument_public_id, timeframe, candle.interval_begin
        )
        topic = self._build_data_topic(
            native_symbol, MarketDataTypeEnum.CANDLES, timeframe=timeframe
        )
        candle_msg = CandleData(
            public_id=public_id,
            timestamp=datetime.now(UTC),
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
            complete=candle.complete,
        )
        row = self._build_candle_row(
            candle_msg,
            instrument_public_id,
            source="synthesized",
            complete=candle.complete,
        )
        await self._publish_message(topic, candle_msg)
        if self._should_persist_row("candles", exchange, native_symbol):
            evicted = _enqueue_or_drop_oldest_candle_write(
                self._candle_write_queue, row, self._get_exchange_name()
            )
            if evicted is not None:
                self._forget_candle_write_sequence(evicted)

    async def _seed_aggregator_from_db(
        self, symbols: list[str], higher: list[str], now: datetime | None = None
    ) -> None:
        """Rebuild each higher timeframe's open (or just-closed) bucket from 1m.

        After a restart the aggregator's buckets are empty, so a mid-window
        restart would otherwise truncate the current higher-TF bar (e.g. a 1d
        bar starting only from the restart time). For each higher timeframe and
        symbol, fold the persisted FINALIZED 1m of the current open window,
        EXCLUDING the current open minute — whose persisted row is a non-final
        update frame and is left for the live stream to finalize. ``get_candles``
        filters ``open_at <= end`` (inclusive), so the read ends one microsecond
        before the current minute. If ANY persisted minute in a window is NON-FINAL
        (its durable ``complete`` flag is False — e.g. an intermediate bar written
        with ``persist_intermediate_candles`` on, or a minute in progress when the
        publisher crashed), the WHOLE window is left unseeded: live frames then
        rebuild it mid-window and the aggregator's complete-window guard suppresses
        it, so a knowingly-incomplete bar is never published (it self-heals on the
        next fully-observed window). This fails safe — the worst case is a
        suppressed first window after a crash, never a stale bar. The check reads
        the persisted ``complete`` column (not a write-time heuristic), so a
        finalized illiquid bar whose last frame arrived before its minute closed —
        sealed ``complete=True`` with a pre-close ``timestamp`` by
        :class:`NativeCandleFinalizer` — is correctly treated as final.

        Known Phase-1 residual: if the seed itself crosses a minute boundary (it
        starts late in a minute and finishes after the next minute has closed),
        the minute that closed mid-seed is neither seeded (excluded as the
        current-at-start minute) nor consumed live (live begins after it closed)
        and was never captured by any consumer — so the higher-TF window closing
        right then can emit one minute short. This is a narrow,
        restart-timing-specific race on a latent publish-only path; the robust
        fix (a durable per-bar completeness signal so such a window is detected
        and suppressed/recovered) is deferred to the persistence phase.

        When the restart lands in the FIRST minute of a window (the current
        window has no completed minutes yet), the immediately-PREVIOUS window is
        rebuilt instead: it closed at-or-just-before the restart and its emit
        (at the boundary minute's finalize) has not happened, so reconstructing
        it lets it publish once the first live minute finalizes — never a
        duplicate, since a window cannot emit during its own first minute.

        No-op without a repository or aggregator. A wildcard (``["*"]``)
        subscription is expanded to the exchange's active instrument list (via
        :meth:`_resolve_seed_symbols`) so a subscribe-all publisher (Kraken spot,
        walutomat) rebuilds its current higher-TF buckets on restart exactly like
        the concrete-symbol publishers — without it the whole current window (most
        visibly the 24h 1d bar) is left unseeded and suppressed until the next
        rollover.

        Args:
            symbols: Symbols to seed.
            higher: Higher timeframe labels being synthesized.
            now: Reference time; defaults to the current UTC time. Injected by
                tests for deterministic window boundaries.
        """
        repository = self.repository
        aggregator = self._candle_aggregator
        if repository is None or aggregator is None:
            return
        now = now if now is not None else datetime.now(UTC)
        minute_floor = CandleAggregator._floor(now, 60)
        end = minute_floor - timedelta(microseconds=1)
        exchange = self._get_exchange_name()
        seed_symbols = await self._resolve_seed_symbols(repository, symbols, exchange, now)
        if not seed_symbols:
            return
        for timeframe in higher:
            await self._seed_timeframe_from_db(
                repository,
                aggregator,
                seed_symbols,
                timeframe,
                now,
                minute_floor,
                end,
                exchange,
            )

    async def _resolve_seed_symbols(
        self,
        repository: Repository,
        symbols: list[str],
        exchange: AllExchange,
        now: datetime,
    ) -> list[str]:
        """Resolve the concrete native symbols whose higher-TF buckets to seed.

        Concrete-symbol subscriptions seed themselves. A wildcard (``["*"]``)
        subscription is expanded to the exchange's active instrument list from the
        durable plane, so a subscribe-all publisher rebuilds its current
        higher-TF buckets on restart instead of leaving the whole current window
        unseeded (which silently stalled the 24h 1d bar for wildcard venues).

        Args:
            repository: Repository used to enumerate active instruments.
            symbols: The publisher's subscription symbol list (possibly ``["*"]``).
            exchange: Exchange whose active instruments to enumerate.
            now: Point-in-time snapshot for the active-instrument read.

        Returns:
            Concrete native symbols to seed; empty when a wildcard expansion finds
            no active instruments.
        """
        if "*" not in symbols:
            return symbols
        active = await repository.get_exchange_instruments(exchange, now)
        if not active:
            logger.warning(
                f"{self.__class__.__name__}: wildcard candle seed found no active instruments "
                f"for {exchange}; current higher-timeframe window left unseeded (it self-heals "
                "on the next fully-observed window)"
            )
            return []
        logger.info(
            f"{self.__class__.__name__}: wildcard candle seed expanded -> {len(active)} symbols"
        )
        return active

    async def _seed_timeframe_from_db(
        self,
        repository: Repository,
        aggregator: CandleAggregator,
        symbols: list[str],
        timeframe: str,
        now: datetime,
        minute_floor: datetime,
        end: datetime,
        exchange: AllExchange,
    ) -> None:
        """Given a timeframe, when restarting, then seed it from finalized 1m rows.

        Args:
            repository: Repository used for persisted 1m reads.
            aggregator: Candle aggregator being rebuilt.
            symbols: Concrete native symbols to seed.
            timeframe: Higher timeframe label to seed.
            now: Restart reference time.
            minute_floor: Current minute floor at restart.
            end: Inclusive read end just before ``minute_floor``.
            exchange: Exchange label used for the persisted read.

        Returns:
            None.

        Raises:
            Exception: Propagates repository read failures unchanged.
        """
        read_start = self._seed_read_start(aggregator, timeframe, now, end)
        if read_start is None:
            return
        for native_symbol in symbols:
            await self._seed_symbol_timeframe_from_db(
                repository,
                aggregator,
                native_symbol,
                timeframe,
                read_start,
                end,
                exchange,
                now,
                minute_floor,
            )

    def _seed_read_start(
        self, aggregator: CandleAggregator, timeframe: str, now: datetime, end: datetime
    ) -> datetime | None:
        """Given a timeframe, when seeding, then calculate the persisted read start.

        Args:
            aggregator: Candle aggregator being rebuilt.
            timeframe: Higher timeframe label to seed.
            now: Restart reference time.
            end: Inclusive read end just before the current minute.

        Returns:
            The read start datetime, or ``None`` for unsupported timeframes.

        Raises:
            This helper does not raise directly.
        """
        window_start = aggregator.window_start(timeframe, now)
        if window_start is None:
            return None
        if window_start <= end:
            return window_start
        return window_start - timedelta(seconds=aggregator.timeframe_seconds(timeframe))

    async def _seed_symbol_timeframe_from_db(
        self,
        repository: Repository,
        aggregator: CandleAggregator,
        native_symbol: str,
        timeframe: str,
        read_start: datetime,
        end: datetime,
        exchange: AllExchange,
        now: datetime,
        minute_floor: datetime,
    ) -> None:
        """Given one symbol/timeframe, when restarting, then fold complete rows.

        Args:
            repository: Repository used for persisted 1m reads.
            aggregator: Candle aggregator being rebuilt.
            native_symbol: Native symbol to seed.
            timeframe: Higher timeframe label to seed.
            read_start: Inclusive read start.
            end: Inclusive read end just before ``minute_floor``.
            exchange: Exchange label used for the persisted read.
            now: Restart reference time used as the repository ``as_of``.
            minute_floor: Current minute floor at restart.

        Returns:
            None.

        Raises:
            Exception: Propagates repository read failures unchanged.
        """
        rows = await repository.get_candles(
            native_symbol, "1m", read_start, end, exchange, now, order="asc"
        )
        candidates = [row for row in rows if row["open_at"] < minute_floor]
        if any(not row["complete"] for row in candidates):
            return
        for row in candidates:
            aggregator.seed_1m(timeframe, self._candle_update_from_row(row, native_symbol))

    @staticmethod
    def _candle_update_from_row(row: CandleRow, symbol: str) -> CandleUpdate:
        """Project a persisted 1m candle row to a :class:`CandleUpdate`.

        Args:
            row: Persisted 1m candle row from :meth:`Repository.get_candles`.
            symbol: Native symbol the row belongs to.

        Returns:
            A 1m :class:`CandleUpdate` suitable for the aggregator seed path.
            A null persisted ``vwap`` falls back to the close price and a null
            ``trades`` to zero, since :class:`CandleUpdate` requires concrete
            values.
        """
        vwap = row["vwap"] if row["vwap"] is not None else row["close"]
        trades = row["trades"] if row["trades"] is not None else 0
        return CandleUpdate(
            symbol=symbol,
            open=row["open"],
            high=row["high"],
            low=row["low"],
            close=row["close"],
            vwap=vwap,
            trades=trades,
            volume=row["volume"],
            interval_begin=row["open_at"],
            interval=60,
        )

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
        probe = get_probe()
        exchange = self._get_data_exchange()
        exchange_label = self._get_exchange_name()
        iterator = self._exchange_client.subscribe_ticks(symbols)
        try:
            t_iter_start = perf_counter_ns()
            async for message in iterator:
                t_after_wait = perf_counter_ns()
                probe.record("iter_wait", t_after_wait - t_iter_start)
                if not self.running:
                    break
                row = await self._process_tick(message, exchange)
                if row is not None and self._should_persist_row("ticks", exchange, message.symbol):
                    _enqueue_or_drop_oldest_tick_write(self._tick_write_queue, row, exchange_label)
                t_iter_end = perf_counter_ns()
                probe.record("tick_iter_total", t_iter_end - t_iter_start)
                probe.maybe_flush()
                t_iter_start = t_iter_end
        except asyncio.CancelledError:
            raise
        except Exception as e:
            logger.error(f"Tick loop error for {symbols}: {e}")

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
        state = _WriterBatchState[TickUpsertRow]([])
        backoff_s = _WRITER_RECONNECT_INITIAL_BACKOFF_S
        while True:
            try:
                async with self._open_tick_writer_session() as writer_session:
                    self._tick_writer_session = writer_session
                    try:
                        await self._drain_writer_session(
                            self._tick_write_queue,
                            state,
                            self._tick_batch_max_rows,
                            _TICK_WRITER_SHUTDOWN_POLL_S,
                            self._flush_tick_writer_batch,
                        )
                    finally:
                        self._tick_writer_session = None
                return
            except _WriterSessionLostError as exc:
                if self._writer_recovery_finished(self._tick_write_queue, state):
                    return
                backoff_s = await self._writer_recovery_backoff(
                    "tick", backoff_s, len(state.batch), exc
                )

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
        the ORM overhead.

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
        desync the ``task_done`` count.

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

        Tick-probe stages recorded here (enabled by
        ``SNAPPER_TICK_PROBE``):

        * ``build_topic`` — topic f-string assembly
        * ``build_tick_model`` — Pydantic ``TickData(...)`` construction
        * ``ensure_instrument`` — symbol → instrument cache lookup
        * ``build_row`` — ``_build_tick_row`` dict assembly
        * ``tick_total`` — end-to-end cost from method entry

        Args:
            message: Raw ticker update from exchange client.
            exchange: Exchange name for message provenance.
        """
        self._last_message_at = monotonic()
        probe = get_probe()
        t_start = perf_counter_ns()
        native_symbol = message.symbol
        received_at = datetime.now(UTC)
        self._last_data_timestamps[native_symbol] = received_at.timestamp() * 1000
        payload_key: tuple[TickPayloadValue, ...] = (
            message.bid,
            message.bid_qty,
            message.ask,
            message.ask_qty,
            message.last,
            message.volume,
            message.vwap,
            message.low,
            message.high,
            message.change,
            message.change_pct,
            message.is_delayed,
            message.is_extended_hours,
        )
        if self._last_tick_payload.get(native_symbol) == payload_key:
            return None
        self._last_tick_payload[native_symbol] = payload_key
        topic = self._build_data_topic(native_symbol, MarketDataTypeEnum.TICKS)
        t_after_topic = perf_counter_ns()
        probe.record("build_topic", t_after_topic - t_start)
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
        t_after_build = perf_counter_ns()
        probe.record("build_tick_model", t_after_build - t_after_topic)
        await self._publish_message(topic, tick_msg)
        t_before_instr = perf_counter_ns()
        instrument_public_id = await self._ensure_instrument(native_symbol)
        t_after_instr = perf_counter_ns()
        probe.record("ensure_instrument", t_after_instr - t_before_instr)
        if instrument_public_id is None:
            probe.record("tick_total", t_after_instr - t_start)
            return None
        row = self._build_tick_row(tick_msg, instrument_public_id)
        t_end = perf_counter_ns()
        probe.record("build_row", t_end - t_after_instr)
        probe.record("tick_total", t_end - t_start)
        return row

    async def _trade_loop(self, symbols: list[str]) -> None:
        """Subscribe to trade data, publish to ZMQ, and hand off DB rows to the writer.

        Ingest-only mirror of :meth:`_tick_loop`:
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
        trade_symbols = self._symbols_for_trade_loop(symbols)
        if not trade_symbols:
            logger.info(f"{self._get_exchange_name()}: Trade loop has no eligible symbols")
            return
        trade_probe = get_trade_probe()
        exchange = self._get_data_exchange()
        exchange_label = self._get_exchange_name()
        iterator = self._exchange_client.subscribe_trades(trade_symbols).__aiter__()
        next_fut: asyncio.Future[TradeUpdate | object] | None = None
        try:
            t_iter_start = perf_counter_ns()
            while self.running:
                next_fut = next_fut or asyncio.ensure_future(anext(iterator, _STREAM_END))
                done, _ = await asyncio.wait({next_fut}, timeout=self._batch_max_age_s)
                if not done:
                    continue
                t_after_wait = perf_counter_ns()
                trade_probe.record("iter_wait", t_after_wait - t_iter_start)
                next_fut = None
                trade = done.pop().result()
                if trade is _STREAM_END:
                    break
                row = await self._process_trade(cast(TradeUpdate, trade), exchange)
                if row is not None and self._should_persist_row(
                    "trades", exchange, cast(TradeUpdate, trade).symbol
                ):
                    _enqueue_or_drop_oldest_trade_write(
                        self._trade_write_queue, row, exchange_label
                    )
                t_iter_end = perf_counter_ns()
                trade_probe.record("trade_iter_total", t_iter_end - t_iter_start)
                trade_probe.maybe_flush()
                t_iter_start = t_iter_end
        except asyncio.CancelledError:
            raise
        except Exception as e:
            logger.error(f"Trade loop error for {trade_symbols}: {e}")
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
        state = _WriterBatchState[TradeUpsertRow]([])
        backoff_s = _WRITER_RECONNECT_INITIAL_BACKOFF_S
        while True:
            try:
                async with self._open_trade_writer_session() as writer_session:
                    self._trade_writer_session = writer_session
                    try:
                        await self._drain_writer_session(
                            self._trade_write_queue,
                            state,
                            self._trade_batch_max_rows,
                            _TRADE_WRITER_SHUTDOWN_POLL_S,
                            self._flush_trade_writer_batch,
                        )
                    finally:
                        self._trade_writer_session = None
                return
            except _WriterSessionLostError as exc:
                if self._writer_recovery_finished(self._trade_write_queue, state):
                    return
                backoff_s = await self._writer_recovery_backoff(
                    "trade", backoff_s, len(state.batch), exc
                )

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

        Records ``writer_flush_total`` on the trade probe (enabled by
        ``SNAPPER_TRADE_PROBE``) covering the full flush cycle: the
        ``_flush_trade_batch`` await plus ``task_done`` bookkeeping
        plus the in-place clear.

        Args:
            batch: In-flight write batch. Cleared in place on a
                successful flush; left empty otherwise.
        """
        if not batch:
            return
        trade_probe = get_trade_probe()
        t_start = perf_counter_ns()
        flushed_count = len(batch)
        await self._flush_trade_batch(batch)
        for _ in range(flushed_count):
            self._trade_write_queue.task_done()
        batch.clear()
        trade_probe.record("writer_flush_total", perf_counter_ns() - t_start)

    async def _process_trade(
        self, trade: TradeUpdate, exchange: MarketDataExchange
    ) -> TradeUpsertRow | None:
        """Build trade message, publish to ZMQ, and return a DB row.

        Publish-first: ZMQ delivery happens before instrument resolution.
        Returns None when instrument is unknown (ZMQ still delivered).

        Trade-probe stages recorded here (enabled by
        ``SNAPPER_TRADE_PROBE``):

        * ``build_topic`` -- topic f-string assembly
        * ``build_trade_model`` -- Pydantic ``TradeData(...)`` construction
        * ``publish`` -- end-to-end ``_publish_message`` cost (model
          copy + JSON serialise + topic validate + socket send)
        * ``ensure_instrument`` -- symbol -> instrument cache lookup
        * ``build_row`` -- ``_build_trade_row`` dict assembly
        * ``trade_total`` -- end-to-end cost from method entry

        Args:
            trade: Raw trade update from exchange client.
            exchange: Exchange name for message provenance.
        """
        self._last_message_at = monotonic()
        trade_probe = get_trade_probe()
        t_start = perf_counter_ns()
        native_symbol = trade.symbol
        received_at = datetime.now(UTC)
        self._last_data_timestamps[native_symbol] = received_at.timestamp() * 1000
        if self._is_duplicate_trade(trade):
            return None
        topic = self._build_data_topic(native_symbol, MarketDataTypeEnum.TRADES)
        t_after_topic = perf_counter_ns()
        trade_probe.record("build_topic", t_after_topic - t_start)
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
        t_after_build = perf_counter_ns()
        trade_probe.record("build_trade_model", t_after_build - t_after_topic)
        await self._publish_message(topic, trade_msg)
        t_after_publish = perf_counter_ns()
        trade_probe.record("publish", t_after_publish - t_after_build)
        instrument_public_id = await self._ensure_instrument(native_symbol)
        t_after_instr = perf_counter_ns()
        trade_probe.record("ensure_instrument", t_after_instr - t_after_publish)
        if instrument_public_id is None:
            trade_probe.record("trade_total", t_after_instr - t_start)
            return None
        row = self._build_trade_row(trade_msg, instrument_public_id)
        t_end = perf_counter_ns()
        trade_probe.record("build_row", t_end - t_after_instr)
        trade_probe.record("trade_total", t_end - t_start)
        return row

    def _is_duplicate_trade(self, trade: TradeUpdate) -> bool:
        """Return True when a trade id was already seen for the same symbol."""
        if trade.trade_id is None:
            return False
        cache = self._seen_trade_ids.get(trade.symbol)
        if cache is None:
            cache = collections.OrderedDict[str, None]()
            self._seen_trade_ids[trade.symbol] = cache
        if trade.trade_id in cache:
            cache.move_to_end(trade.trade_id)
            return True
        cache[trade.trade_id] = None
        while len(cache) > _TRADE_ID_LRU_MAX_PER_SYMBOL:
            cache.popitem(last=False)
        return False

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
                await self._heartbeat_tick(component_name)
            except FeedDarkTooLongError:
                raise
            except Exception as e:
                logger.error(f"Heartbeat error: {e}")

    async def _heartbeat_tick(self, component_name: str) -> None:
        """Compute and publish one heartbeat with honest status.

        Runs the full per-tick sequence: bump the sequence counter,
        compute the data-lag figure, run the dark-feed liveness guard,
        then build and publish the heartbeat message. Status computation
        must never be skipped or reordered (#145 P2-1 honest heartbeats).

        Args:
            component_name: Heartbeat component identity for topic and
                message provenance.

        Raises:
            FeedDarkTooLongError: When the liveness guard decides the
                feed has been dark beyond the exit ceiling.
        """
        self.heartbeat_seq += 1
        max_lag_ms = self._compute_max_lag_ms()
        self._check_feed_liveness()
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

    def _compute_max_lag_ms(self) -> int:
        """Compute the worst per-symbol data lag in milliseconds.

        Symbols that have never delivered data count as zero lag (their
        last-data timestamp defaults to the current time).

        Returns:
            Maximum lag across all subscribed symbols, in milliseconds.
        """
        current_time = datetime.now(UTC).timestamp() * 1000
        max_lag_ms = 0
        for symbol in self.symbols:
            last_data_time = self._last_data_timestamps.get(symbol, current_time)
            lag_ms = int(current_time - last_data_time)
            max_lag_ms = max(max_lag_ms, lag_ms)
        return max_lag_ms

    def _check_feed_liveness(self) -> None:
        """Run the dark-feed liveness guard for the current tick.

        When the recovery threshold is enabled and no message has
        arrived for longer than it, spawn a recovery attempt; when the
        feed stays dark past ``_DARK_FEED_EXIT_CEILING_S`` despite
        recovery, escalate to a process exit.

        Separately, for venues with a dedicated live candle source
        (``_candle_liveness_threshold_s() > 0``) that are actually consuming
        one (``_consumes_native_candles`` — guards the degenerate
        ``timeframes=[]`` config where no candle is ever subscribed, which
        would otherwise churn), spawn a recovery when no live candle has
        arrived venue-wide for longer than that threshold even though ticks
        or trades keep the shared message watchdog satisfied. This catches a silent candle-source stall;
        it never escalates to a process exit, because a candle-only stall
        with a live trade channel must not kill the trade feed — the
        WS-restart recovery re-subscribes the dead channel instead.

        On a candle venue BOTH triggers spawn with
        ``require_candle_progress=True`` so recovery is only declared
        successful once a live candle resumes (not merely trades). This
        also closes a general-outage race: after a long full-dark recovery
        a candle-blind success would release the lock while candles are
        still arriving, and the candle branch would immediately restart the
        just-restored socket.

        Raises:
            FeedDarkTooLongError: When the feed has been dark beyond the
                exit ceiling and the launcher must restart the publisher.
        """
        candle_threshold_s = self._candle_liveness_threshold_s()
        candle_venue = candle_threshold_s > 0 and self._consumes_native_candles
        threshold_s = self._get_liveness_recovery_threshold_s()
        if threshold_s > 0:
            stale_for = monotonic() - self._last_message_at
            if stale_for > threshold_s:
                self._spawn_recovery(
                    reason=f"no_messages_for_{stale_for:.0f}s",
                    require_candle_progress=candle_venue,
                )
            if stale_for > _DARK_FEED_EXIT_CEILING_S:
                logger.error(
                    f"{self._get_exchange_name()}: feed dark for {stale_for:.0f}s "
                    f"(> {_DARK_FEED_EXIT_CEILING_S:.0f}s ceiling) despite recovery; "
                    "exiting for launcher restart"
                )
                raise FeedDarkTooLongError(f"{self._get_exchange_name()} dark for {stale_for:.0f}s")
        if candle_venue:
            candle_stale_for = monotonic() - self._last_candle_msg_at
            if candle_stale_for > candle_threshold_s:
                self._spawn_recovery(
                    reason=f"no_candles_for_{candle_stale_for:.0f}s",
                    require_candle_progress=True,
                )

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
        self,
        candle_msg: CandleData,
        instrument_public_id: str,
        *,
        source: str = "native",
        complete: bool = True,
    ) -> CandleUpsertRow:
        """Build a CandleUpsertRow from a published CandleData message.

        Args:
            candle_msg: Published CandleData with OHLCV fields.
            instrument_public_id: Resolved instrument identity.
            source: Provenance tag — ``native`` for exchange-native frames
                (all 1m), ``synthesized`` for aggregator higher-TF rollups.
            complete: Whether the bar's window has closed. For native frames it
                carries the window-closed flag (False for an in-progress minute,
                True once elapsed / when sealed by the finalizer); for synthesized
                bars it carries the aggregator bucket's trustworthy-boundary flag.

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
            "source": source,
            "complete": complete,
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

        Mirrors :meth:`_flush_tick_batch`'s writer-session pattern:
        when the candle writer task injected a pinned session via
        :meth:`_open_candle_writer_session`, the upsert runs on that
        session without acquiring a fresh DBAPI connection per
        flush, and this method commits explicitly afterwards.

        Args:
            batch: List of candle rows to persist.
        """
        if not batch:
            return
        repository = self._require_repository()
        try:
            writer_session = self._candle_writer_session
            if writer_session is not None:
                await repository.upsert_candles(batch, session=writer_session)
                await writer_session.commit()
            else:
                await repository.upsert_candles(batch)
            self._mark_candle_writes_committed(batch)
            self._flush_errors["candle"] = 0
        except IntegrityError:
            writer_session = self._candle_writer_session
            if writer_session is not None:
                with contextlib.suppress(Exception):
                    await writer_session.rollback()
            await self._flush_candle_batch_row_by_row(batch)
        except Exception as e:
            self._flush_errors["candle"] += 1
            logger.error(f"Candle batch flush failed ({len(batch)} rows): {e}")
            writer_session = self._candle_writer_session
            if writer_session is not None:
                with contextlib.suppress(Exception):
                    await writer_session.rollback()
            if _is_disconnect_error(e):
                raise _WriterSessionLostError(str(e)) from e
            self._forget_candle_writes(batch)

    async def _flush_single_candle_row(self, row: CandleUpsertRow) -> bool:
        """Flush one candle row during row-by-row fallback.

        Args:
            row: Candle row to persist.

        Returns:
            True when the row hit a generic non-disconnect failure,
            otherwise False.

        Raises:
            _WriterSessionLostError: If the row failure indicates a lost
                writer session.
        """
        repository = self._require_repository()
        writer_session = self._candle_writer_session
        try:
            if writer_session is not None:
                await repository.upsert_candles([row], session=writer_session)
                await writer_session.commit()
            else:
                await repository.upsert_candles([row])
            self._mark_candle_writes_committed([row])
            return False
        except IntegrityError as exc:
            logger.warning(
                f"Candle upsert skipped: instrument={row.get('instrument_public_id')}, "
                f"open_at={row.get('open_at')}, error={exc}"
            )
            if writer_session is not None:
                with contextlib.suppress(Exception):
                    await writer_session.rollback()
            self._forget_candle_write_sequence(row)
            return False
        except Exception as e:
            self._flush_errors["candle"] += 1
            logger.error(f"Candle row flush failed: {e}")
            if writer_session is not None:
                with contextlib.suppress(Exception):
                    await writer_session.rollback()
            if _is_disconnect_error(e):
                raise _WriterSessionLostError(str(e)) from e
            self._forget_candle_write_sequence(row)
            return True

    @staticmethod
    def _retain_unfinished_candle_rows(
        batch: list[CandleUpsertRow],
        snapshot: list[CandleUpsertRow],
        committed_or_skipped: set[int],
    ) -> None:
        """Trim a candle batch to rows that still need retry after disconnect.

        Args:
            batch: Original mutable batch to trim in place.
            snapshot: Stable batch snapshot used for index-based trimming.
            committed_or_skipped: Row indexes that no longer need retry.
        """
        retry_idx = set(range(len(snapshot))) - committed_or_skipped
        batch[:] = [snapshot[i] for i in sorted(retry_idx)]

    async def _flush_candle_batch_row_by_row(self, batch: list[CandleUpsertRow]) -> None:
        """Fallback: retry each candle individually to isolate bad rows.

        Disconnect handling: tracks **per-row** outcome via an index
        set so the trimmed ``batch`` contains exactly the rows that
        still need to be retried. Generic non-disconnect failures and
        successful commits are both marked as "no longer retry-able"
        (committed-or-skipped) so the trim only keeps the disconnect
        row. Example: row 0 succeeds, row 1 hits a generic timeout
        (logged + dropped), row 2 succeeds, row 3 disconnects →
        trimmed batch keeps ONLY row 3. A position-based ``pop`` of
        the committed prefix only would incorrectly leave the
        already-committed row 2 in the batch where it would be
        replayed through the SCD2 close-old + insert-new pipeline.
        On disconnect, raises :class:`_WriterSessionLostError` so the
        writer loop can dispose the dead session and re-open.

        Queue ``task_done`` accounting is the writer loop's job; see
        :meth:`_flush_candle_writer_batch` for the matching prefix
        acknowledgement. Doing the bookkeeping there keeps this
        helper safe to call directly from tests / ad-hoc flushes.

        Generic non-disconnect errors (statement timeout on a
        still-live session, etc.) are logged + rolled back per row
        and the failed row is dropped silently — this matches the
        legacy pre-2026-05-28 behavior here and is a known limitation
        (not introduced by the writer-recovery work). No retry policy
        distinguishes "transient drop" from "permanent drop" — naive
        per-row retries risk livelocking shutdown — so persistent
        generic failures produce repeated ERROR log lines per row but
        do not block ``queue.join``.

        Args:
            batch: List of candle rows where batch upsert failed.
                Mutated in place on disconnect: rebuilt from rows
                whose index was NOT in the committed-or-skipped set.
        """
        self._require_repository()
        had_generic_error = False
        committed_or_skipped: set[int] = set()
        snapshot = list(batch)
        for idx, row in enumerate(snapshot):
            try:
                had_generic_error = await self._flush_single_candle_row(row) or had_generic_error
            except _WriterSessionLostError:
                self._retain_unfinished_candle_rows(batch, snapshot, committed_or_skipped)
                raise
            committed_or_skipped.add(idx)
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
        repository = self._require_repository()
        try:
            writer_session = self._tick_writer_session
            if writer_session is not None:
                await repository.upsert_ticks(batch, session=writer_session)
                await writer_session.commit()
            else:
                await repository.upsert_ticks(batch)
            self._flush_errors["tick"] = 0
        except Exception as e:
            self._flush_errors["tick"] += 1
            logger.error(f"Tick batch flush failed ({len(batch)} rows): {e}")
            writer_session = self._tick_writer_session
            if writer_session is not None:
                with contextlib.suppress(Exception):
                    await writer_session.rollback()
            if _is_disconnect_error(e):
                raise _WriterSessionLostError(str(e)) from e

    async def _flush_trade_batch(self, batch: list[TradeUpsertRow]) -> None:
        """Flush trade batch to DB (ON CONFLICT DO NOTHING).

        Mirrors :meth:`_flush_tick_batch`'s writer-session pattern:
        when the trade writer task injected a pinned session via
        :meth:`_open_trade_writer_session`, the upsert runs on that
        session without acquiring a fresh DBAPI connection per
        flush, and this method commits explicitly afterwards.

        Records ``writer_upsert_call`` on the trade probe (enabled by
        ``SNAPPER_TRADE_PROBE``) covering just the
        ``upsert_trades + commit`` await pair so the operator can
        attribute writer-side latency to the DB I/O round-trip vs the
        surrounding bookkeeping captured by ``writer_flush_total``.

        Args:
            batch: List of trade rows to persist.
        """
        if not batch:
            return
        repository = self._require_repository()
        trade_probe = get_trade_probe()
        t_start = perf_counter_ns()
        try:
            writer_session = self._trade_writer_session
            if writer_session is not None:
                await repository.upsert_trades(batch, session=writer_session)
                await writer_session.commit()
            else:
                await repository.upsert_trades(batch)
            self._flush_errors["trade"] = 0
            trade_probe.record("writer_upsert_call", perf_counter_ns() - t_start)
        except Exception as e:
            self._flush_errors["trade"] += 1
            logger.error(f"Trade batch flush failed ({len(batch)} rows): {e}")
            writer_session = self._trade_writer_session
            if writer_session is not None:
                with contextlib.suppress(Exception):
                    await writer_session.rollback()
            if _is_disconnect_error(e):
                raise _WriterSessionLostError(str(e)) from e

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

    async def _feed_health_flush_loop(self) -> None:
        """Periodically persist the exchange client's feed-health snapshot.

        Runs every :data:`_FEED_HEALTH_FLUSH_INTERVAL_S` while the
        publisher is running, snapshotting the in-memory subscription
        tracker and upserting its current state into the
        ``instrument_feed_health`` table so operators can query it after
        the fact. The loop is fully defensive: a flush failure is logged
        and the loop continues — feed-health persistence never crashes
        the publisher hot path.

        Args:
            None.

        Returns:
            None.
        """
        exchange = self._get_exchange_name()
        logger.info(f"{exchange}_feed_publisher: Starting feed-health flush loop")
        while self.running:
            await asyncio.sleep(_FEED_HEALTH_FLUSH_INTERVAL_S)
            if not self.running:
                break
            await self._flush_feed_health()

    async def _flush_feed_health(self) -> None:
        """Snapshot the tracker and upsert its wall-clock current state.

        Wrapped in a broad try/except: any failure (no client / no repo /
        DB error) is logged and swallowed so the flush loop survives and
        the publisher hot path is never affected.

        Args:
            None.

        Returns:
            None.
        """
        try:
            client = self._exchange_client
            repository = self.repository
            if client is None or repository is None:
                return
            snapshot = client.subscription_health_snapshot()
            if not snapshot:
                return
            try:
                await self._after_feed_health_snapshot(snapshot)
            except Exception as exc:
                logger.error(
                    f"{self._get_exchange_name()}_feed_publisher: feed-health hook failed: {exc}"
                )
            rows = self._build_feed_health_rows(snapshot)
            await repository.upsert_instrument_feed_health(rows)
        except Exception as exc:
            logger.error(
                f"{self._get_exchange_name()}_feed_publisher: feed-health flush failed: {exc}"
            )

    async def _after_feed_health_snapshot(
        self, snapshot: dict[tuple[str, str], _SymbolEntry]
    ) -> None:
        """Handle a feed-health snapshot before it is persisted.

        The base implementation is an awaitable no-op so subclasses can
        override the hook with asynchronous persistence or rebroadcast work
        while callers use one consistent await site.

        Args:
            snapshot: Point-in-time subscription-health entries.

        Returns:
            None.
        """
        await asyncio.sleep(0)

    def _build_feed_health_rows(
        self, snapshot: dict[tuple[str, str], _SymbolEntry]
    ) -> list[InstrumentFeedHealthUpsertRow]:
        """Convert a tracker snapshot to wall-clock upsert rows.

        The tracker stores ``requested_at`` / ``confirmed_at`` /
        ``last_seen_data_at`` as :func:`time.monotonic` values, which are
        meaningless as persisted timestamps. A single ``wall_now`` /
        ``mono_now`` pair is captured once per flush so every monotonic
        value ``m`` maps to the same reference instant via
        ``wall_now - timedelta(seconds=(mono_now - m))``; ``None`` stays
        ``None``.

        Args:
            snapshot: Mapping from ``(channel, symbol)`` to the tracker's
                copied entry objects.

        Returns:
            One :class:`InstrumentFeedHealthUpsertRow` per snapshot entry,
            tagged with this coordinator and exchange.
        """
        wall_now = datetime.now(UTC)
        mono_now = monotonic()
        coordinator = f"coord-{self.settings.coordinator_instance_id}"
        exchange = self._get_exchange_name()
        rows: list[InstrumentFeedHealthUpsertRow] = []
        for entry in snapshot.values():
            rows.append(
                InstrumentFeedHealthUpsertRow(
                    coordinator=coordinator,
                    exchange=exchange,
                    channel=entry.channel,
                    symbol=entry.symbol,
                    status=entry.status,
                    requested_at=self._monotonic_to_wall(entry.requested_at, wall_now, mono_now),
                    confirmed_at=self._monotonic_to_wall_optional(
                        entry.confirmed_at, wall_now, mono_now
                    ),
                    last_seen_data_at=self._monotonic_to_wall_optional(
                        entry.last_seen_data_at, wall_now, mono_now
                    ),
                    last_error=entry.last_error,
                    retry_count=entry.retry_count,
                    snapshot_at=wall_now,
                )
            )
        return rows

    @staticmethod
    def _monotonic_to_wall(value: float, wall_now: datetime, mono_now: float) -> datetime:
        """Convert one monotonic value to wall-clock against a flush reference.

        Args:
            value: A :func:`time.monotonic` reading captured by the
                tracker.
            wall_now: Wall-clock instant captured once per flush.
            mono_now: Monotonic reading captured at the same flush instant
                as ``wall_now``.

        Returns:
            The wall-clock instant corresponding to ``value``.
        """
        return wall_now - timedelta(seconds=(mono_now - value))

    @classmethod
    def _monotonic_to_wall_optional(
        cls, value: float | None, wall_now: datetime, mono_now: float
    ) -> datetime | None:
        """Convert an optional monotonic value, preserving ``None``.

        Args:
            value: A :func:`time.monotonic` reading or ``None``.
            wall_now: Wall-clock instant captured once per flush.
            mono_now: Monotonic reading captured at the same flush instant
                as ``wall_now``.

        Returns:
            The wall-clock instant for ``value``, or ``None`` when
            ``value`` is ``None``.
        """
        if value is None:
            return None
        return cls._monotonic_to_wall(value, wall_now, mono_now)

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
