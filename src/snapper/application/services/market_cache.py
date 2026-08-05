"""MarketCacheService — in-process 1-minute candle cache.

Holds the most recent N closed 1m candles for every instrument the
publishers stream so chart routes can serve sub-millisecond reads
without hitting the database. The cache is fed live from the broker's
market.* topics; persisted instruments are additionally pre-warmed
from :class:`Repository` at lifespan start so a freshly-rebooted
server has chart data immediately without waiting on the first close.

Design constraints:

- **1-minute candles only.** Other timeframes either derive from the
  1m deque (5m, 15m, 30m via the read route) or fall through to the
  DB (1h, 4h, 1d). The cache MUST refuse to mix timeframes inside the
  per-instrument deque.
- **Closed bars only.** :class:`CandleData` carries no ``closed``
  flag; closure is derived from the ``open_at`` transition. An
  in-flight bar lives in :attr:`_current` until a frame with a newer
  ``open_at`` arrives, at which point the prior :attr:`_current` is
  promoted to the deque.
- **All mutations under one** :class:`asyncio.Lock`. Reads use the
  same lock to give callers a coherent snapshot of the deque tuple
  rather than a partially-mutated state mid-promote.
- **Stale-key prune.** A 60-second background loop drops instruments
  not seen in 6 hours, freeing memory for cold instruments without
  blocking the hot path.
"""

import asyncio
import contextlib
import time
from collections import deque
from collections.abc import Sequence
from dataclasses import dataclass
from datetime import UTC
from datetime import datetime
from datetime import timedelta
from typing import Final
from typing import Literal

import zmq
import zmq.asyncio
from loguru import logger

from snapper.application.services.market_persist_policy import MarketPersistPolicy
from snapper.core.types import AllExchange
from snapper.data.repository import Repository
from snapper.data.repository_types import CandleRow
from snapper.messaging.infrastructure.validated_socket import HWM_MARKET_DATA
from snapper.messaging.infrastructure.validated_socket import ValidatedSubscriber
from snapper.messaging.infrastructure.validated_socket import apply_hwm
from snapper.messaging.schemas.data import CandleData

_CACHE_CANDLE_LIMIT = 100
"""Per-instrument deque cap. 100 minutes is enough to derive 30m × 3
cells for the chart route while keeping cache memory predictable
(~100 instruments × 100 candles × ~96 bytes = ~1 MB)."""

_ELAPSED_CURRENT_GRACE_S = 5.0
"""Seconds past a 1m window's end before the in-flight bar may be read.

Mirrors ``_CANDLE_FLUSH_GRACE_S`` in the publisher, whose rationale — typical
Kraken OHLC delivery latency — applies unchanged here.

Deliberately NOT keyed to the write-side ceiling. With minute completion active
the publisher's own seal waits ``trade_built_finalize_grace_seconds`` (12) plus
``MINUTE_SWEEP_MARGIN_S`` (8) plus this grace, so a read that never exposed a
bar the writer might still revise would have to wait 25 s, and more if an
operator raises the finalize grace. That trade is not worth taking: a bar
exposed here can still be revised, which is exactly what the live chart already
does with every in-progress frame it receives over the bus. The candidate stays
in ``_current`` rather than being promoted, so a later frame corrects it and the
next read sees the correction."""

_MINUTE_MS: Final[int] = 60_000
"""One 1m window width in integer milliseconds."""

_PRUNE_LOOP_INTERVAL_S = 60.0
"""How often the stale-key prune loop walks ``_last_seen_at``."""

_STALE_AFTER_S = 6 * 60 * 60
"""Drop instruments not seen in this many seconds. 6 hours covers
overnight kraken-spot lulls without losing prewarmed memory."""

_TARGET_TIMEFRAME = "1m"
"""Cache only ingests the 1m timeframe; other frames are filtered out."""

_LISTEN_RECV_BACKOFF_S = 0.1
"""Backoff after a transient ``recv_multipart`` failure."""

_MARKET_TOPIC_PREFIX = "market."
"""ZMQ SUB filter prefix. Narrower filters (``.candles.1m``) live
inside :meth:`_should_ingest_topic` because ZMQ filters match bytes
at a fixed offset and ``.candles.1m`` sits at a variable offset."""

_TARGET_TOPIC_SUFFIX = ".candles.1m"
"""Python-side suffix gate so the SUB socket can use the cheap
``market.`` byte prefix without parsing every tick / trade payload."""

_PREWARM_MAX_CONCURRENCY: Final = 4
"""Concurrent prewarm slices, following the house pattern in
``messaging/publishers/base.py``.

Sized against the API container, which is the only one that builds a
:class:`MarketCacheService` and which runs the SQLAlchemy default pool
of 5 with 10 overflow. It is deliberately NOT sized against the clamped
feed / strategies / notify containers (``DB_POOL_SIZE=2``), which never
prewarm. Four leaves a connection free for the rest of lifespan startup
while staying well inside the pool, which matters because
``pool_timeout`` is configured nowhere in this codebase: exhaustion
would stall 30 s per slice and surface as a swallowed ``TimeoutError``
rather than a hard failure. Lower this to 2 if the API container ever
inherits the clamp."""

_PREWARM_INSTRUMENT_SLICE: Final = 64
"""Instruments read per session. Bounds both the blast radius of a
slice-level failure and the work re-done when a slice falls back to
the per-instrument path."""

_PREWARM_LOOKBACK: Final = timedelta(days=2)
"""First-pass ``open_at`` floor, mirroring
``repository._CANDLE_ID_CACHE_LOOKBACK``. ``_CACHE_CANDLE_LIMIT`` 1m
bars span 100 minutes, so two days leaves ~29x slack for any
instrument with recent trading. Instruments the floored pass leaves
short are re-read with NO floor, so the bound can never shrink
coverage."""

_PREWARM_DEADLINE_S: Final = 60.0
"""Hard wall-clock ceiling on the whole prewarm phase.

Prewarm sits on the lifespan critical path, so its cost is startup
latency and the container's healthcheck budget is finite. Once this
elapses, remaining reads are abandoned and the cache warms from live
ingest instead. Without it a systemically unavailable database turns
every read into a 30 s pool timeout and prewarm degrades into a
multi-hour stall — the exact failure this batching work exists to
prevent, only worse."""

_PREWARM_SLICE_FAILURE_BUDGET: Final = 3
"""Slice failures tolerated before the per-instrument fallback stops.

The fallback exists so ONE poisoned symbol cannot cost its whole slice.
It is the wrong response to a dead database, where it would multiply a
single failed batch read into ``_PREWARM_INSTRUMENT_SLICE`` further
failing reads. Past this many failed slices the failure is treated as
systemic and remaining slices fail fast."""

PrewarmOutcome = Literal["warmed", "empty", "failed"]
"""Per-instrument prewarm result. ``"empty"`` (no 1m history) and
``"failed"`` (the DB call raised) were previously conflated into a
single ``False``, which is why an observed warmed-vs-configured gap
could not be interpreted."""


@dataclass(frozen=True, slots=True)
class CandleSnap:
    """Compact frozen snapshot of one closed 1m candle.

    Stores ``open_at`` as an integer millisecond epoch so deque
    membership checks + chronological dedup avoid the cost of
    constructing :class:`datetime` instances on every frame.

    Attributes:
        open_at_ms: Candle interval start as integer milliseconds
            since the UTC epoch.
        open: Opening price.
        high: Highest price during the candle.
        low: Lowest price during the candle.
        close: Closing price.
        volume: Total traded volume.
    """

    open_at_ms: int
    open: float
    high: float
    low: float
    close: float
    volume: float


@dataclass(slots=True)
class _PrewarmBudget:
    """Shared time and failure budget bounding one prewarm run.

    Both bounds exist to keep a degraded database from converting
    prewarm into an unbounded startup stall: the deadline caps total
    wall time, and the failure counter stops the per-instrument fallback
    once failures look systemic rather than symbol-specific.

    Attributes:
        deadline: Event-loop time after which no further read starts.
        slice_failures: Slice-level failures observed so far.
    """

    deadline: float
    slice_failures: int = 0

    def exhausted(self, now: float) -> bool:
        """Return whether prewarm must stop starting new reads.

        Args:
            now: Current event-loop time.

        Returns:
            Whether either bound has been reached.
        """
        return now >= self.deadline or self.slice_failures >= _PREWARM_SLICE_FAILURE_BUDGET


@dataclass(frozen=True, slots=True)
class _PrewarmTarget:
    """One resolved instrument queued for prewarm.

    Attributes:
        exchange: Source exchange identifier.
        native_symbol: Native-format symbol (never the literal ``"*"``).
        instrument_public_id: Public ID resolved in batch per exchange,
            so the candle read never re-resolves the symbol.
    """

    exchange: AllExchange
    native_symbol: str
    instrument_public_id: str


@dataclass(slots=True)
class _PrewarmRun:
    """Mutable progress shared by the two bounded prewarm passes."""

    targets: list[_PrewarmTarget]
    as_of: datetime
    semaphore: asyncio.Semaphore
    budget: _PrewarmBudget
    counts: dict[tuple[AllExchange, str], int]
    raised: set[tuple[AllExchange, str]]


@dataclass(frozen=True, slots=True)
class PairStats:
    """Computed Pearson + Engle-Granger cointegration for a pair of legs.

    Populated by :class:`snapper.application.services.market_stats.MarketStatsWorker`
    on its 60 s (Pearson) and 300 s (cointegration) cadences. Routes
    serialize this directly into the stats response envelope.

    Attributes:
        pearson_r: Pearson correlation coefficient on the latest
            aligned-close sample (``None`` until first compute).
        pearson_n: Sample count contributing to ``pearson_r``.
        coint_t: Engle-Granger ``coint_t`` statistic (``None`` until
            the cointegration cadence has produced a value).
        coint_pvalue: Engle-Granger asymptotic p-value (``None`` until
            first compute).
        coint_critical_values: Critical-value triple (1%, 5%, 10%)
            from ``statsmodels.tsa.stattools.coint``.
        computed_at: UTC timestamp of the most recent successful
            compute (whichever cadence ran last).
        sample_count: Sample size of the most recent compute.
        is_warm: ``True`` once a sample exceeds the per-cadence
            threshold (≥ 30 for Pearson, ≥ 60 for cointegration).
    """

    pearson_r: float | None = None
    pearson_n: int = 0
    coint_t: float | None = None
    coint_pvalue: float | None = None
    coint_critical_values: tuple[float, float, float] | None = None
    computed_at: datetime | None = None
    sample_count: int = 0
    is_warm: bool = False


def _snap_from_candle_data(candle: CandleData) -> CandleSnap:
    """Project a :class:`CandleData` ZMQ frame to a :class:`CandleSnap`."""
    return CandleSnap(
        open_at_ms=int(candle.open_at.timestamp() * 1000),
        open=candle.open,
        high=candle.high,
        low=candle.low,
        close=candle.close,
        volume=candle.volume,
    )


def _snap_from_candle_row(row: CandleRow) -> CandleSnap:
    """Project a :class:`CandleRow` DB row to a :class:`CandleSnap`."""
    return CandleSnap(
        open_at_ms=int(row["open_at"].timestamp() * 1000),
        open=row["open"],
        high=row["high"],
        low=row["low"],
        close=row["close"],
        volume=row["volume"],
    )


class MarketCacheService:
    """In-process 1m candle cache fed by the market.* broker topics.

    Instantiated once per process from the FastAPI lifespan AFTER
    :class:`MarketPersistPolicy` is ready (the cache's prewarm path
    asks the policy which instruments to fetch from the DB) and
    BEFORE :meth:`ProcessLauncherService.start_all_processes` so the
    SUB socket is connected to the broker before the first candle
    frame leaves a publisher.

    Reads (:meth:`get_1m_candles`, :meth:`current_for`) take the same
    :attr:`_lock` as mutations so callers see a coherent snapshot of
    each per-instrument deque rather than a partially-mutated state
    mid-promote.

    Attributes:
        repository: Source of the prewarm candle backfill.
        persist_policy: Drives the prewarm instrument set.
    """

    def __init__(
        self,
        repository: Repository,
        persist_policy: MarketPersistPolicy,
    ) -> None:
        """Initialise empty state. :meth:`start` performs the heavy work.

        Args:
            repository: Async repository handle for the prewarm path.
            persist_policy: Configured policy supplying
                :meth:`iter_persisted_instruments`.
        """
        self.repository = repository
        self.persist_policy = persist_policy
        self._lock = asyncio.Lock()
        self._candles: dict[tuple[AllExchange, str], deque[CandleSnap]] = {}
        self._current: dict[tuple[AllExchange, str], CandleSnap] = {}
        self._last_seen_at: dict[tuple[AllExchange, str], float] = {}
        self._stats: dict[tuple[str, str], PairStats] = {}
        self._zmq_context: zmq.asyncio.Context | None = None
        self._subscriber: ValidatedSubscriber | None = None
        self._ingest_task: asyncio.Task[None] | None = None
        self._prune_task: asyncio.Task[None] | None = None
        self._running = False
        self._listener_lock = asyncio.Lock()

    async def start(self, zmq_broker_xpub: str) -> None:
        """Open the SUB socket, prewarm from DB, and spawn the ingest + prune tasks.

        Idempotent + restart-safe via :attr:`_listener_lock`. Empty
        ``zmq_broker_xpub`` skips socket setup entirely (test mode);
        the cache still serves prewarmed data + accepts manual writes
        through :meth:`record_candle_for_test`.

        Args:
            zmq_broker_xpub: Address of the broker's XPUB endpoint.
                Empty string skips the listener.
        """
        async with self._listener_lock:
            if self._ingest_task is not None and not self._ingest_task.done():
                return
            if self._ingest_task is not None:
                await self._reap_unlocked()
            await self._prewarm()
            if not zmq_broker_xpub:
                logger.info("MarketCacheService: empty broker XPUB, ingest task skipped")
                self._prune_task = asyncio.create_task(self._prune_loop())
                return
            self._zmq_context = zmq.asyncio.Context()
            raw_sub_socket = self._zmq_context.socket(zmq.SUB)
            apply_hwm(raw_sub_socket, rcvhwm=HWM_MARKET_DATA)
            raw_sub_socket.connect(zmq_broker_xpub)
            self._subscriber = ValidatedSubscriber(raw_sub_socket)
            self._subscriber.subscribe(_MARKET_TOPIC_PREFIX)
            self._running = True
            self._ingest_task = asyncio.create_task(self._ingest_loop())
            self._prune_task = asyncio.create_task(self._prune_loop())
            logger.info(
                "MarketCacheService: subscribed to {}* on {} (prewarmed {} instruments)",
                _MARKET_TOPIC_PREFIX,
                zmq_broker_xpub,
                len(self._candles),
            )

    async def stop(self) -> None:
        """Cancel ingest + prune tasks and dispose ZMQ resources. Idempotent."""
        async with self._listener_lock:
            await self._reap_unlocked()

    async def _reap_unlocked(self) -> None:
        """Tear down resources. Caller MUST hold :attr:`_listener_lock`."""
        self._running = False
        ingest_task = self._ingest_task
        prune_task = self._prune_task
        subscriber = self._subscriber
        context = self._zmq_context
        self._ingest_task = None
        self._prune_task = None
        self._subscriber = None
        self._zmq_context = None
        for task in (ingest_task, prune_task):
            if task is None:
                continue
            task.cancel()
            with contextlib.suppress(asyncio.CancelledError, Exception):
                await task
        if subscriber is not None:
            with contextlib.suppress(Exception):
                subscriber.close()
        if context is not None:
            with contextlib.suppress(Exception):
                context.term()

    async def get_1m_candles(
        self,
        exchange: AllExchange,
        native_symbol: str,
        *,
        limit: int,
        include_elapsed_current: bool = False,
    ) -> list[CandleSnap]:
        """Return up to ``limit`` most-recent closed 1m candles, chronological.

        Returns an empty list when the cache has never seen this
        instrument or when prewarm did not surface it. Callers (the
        read route) decide whether to fall through to the DB or
        return an empty payload with ``is_warm=false``.

        Args:
            exchange: Source exchange.
            native_symbol: Native-format symbol (e.g. ``"BTC-USD"``).
            limit: Maximum number of bars; pulled from the right side
                of the deque so the most recent bars are always
                included.
            include_elapsed_current: Also return the in-flight bar once its
                own window has ended. Defaults to False, and every caller
                except the public candle facade leaves it there: the cache
                diagnostic route exists to report the deque literally, and
                :mod:`candle_coverage` compares the deque against the DB, so
                a bar the deque does not hold must not appear for them.

        Returns:
            List of :class:`CandleSnap` in chronological order
            (oldest first). Length ≤ ``limit``.
        """
        key = (exchange, native_symbol)
        async with self._lock:
            cached = self._candles.get(key)
            snaps = [] if cached is None else list(cached)
            if include_elapsed_current:
                snaps = self._merge_elapsed_current(snaps, key)
            return snaps if limit >= len(snaps) else snaps[-limit:]

    def _merge_elapsed_current(
        self, snaps: list[CandleSnap], key: tuple[AllExchange, str]
    ) -> list[CandleSnap]:
        """Append the in-flight bar once its own window has demonstrably ended.

        The deque holds a bar only once a STRICTLY NEWER one arrives, so for an
        instrument that trades every few hours its newest closed bar stays
        invisible for exactly that long. Reading it back here is what makes the
        served series reach the present.

        The test is on the bar's own window, not on the arrival of anything
        else: a bar stops being in flight when its minute ends, and those are
        the same statement only for an instrument that trades every minute.

        Caller holds :attr:`_lock`.

        Args:
            snaps: Closed bars already taken from the deque.
            key: Exchange and native symbol being read.

        Returns:
            ``snaps`` with the in-flight bar appended, or replacing an equal
            tail, when its window ended more than
            :data:`_ELAPSED_CURRENT_GRACE_S` ago. Unchanged otherwise.
        """
        current = self._current.get(key)
        if current is None:
            return snaps
        ends_at = (current.open_at_ms + _MINUTE_MS) / 1000.0
        if time.time() < ends_at + _ELAPSED_CURRENT_GRACE_S:
            return snaps
        if snaps and snaps[-1].open_at_ms == current.open_at_ms:
            return [*snaps[:-1], current]
        if snaps and snaps[-1].open_at_ms > current.open_at_ms:
            return snaps
        return [*snaps, current]

    async def instruments_cached(self) -> int:
        """Return the count of distinct ``(exchange, symbol)`` keys in the cache.

        Returns:
            Number of instruments with at least one closed bar on the deque.
        """
        async with self._lock:
            return len(self._candles)

    async def get_pair_stats(self, left_key: str, right_key: str) -> PairStats | None:
        """Return the computed stats for ``(left_key, right_key)`` or ``None``.

        Pair keys are the canonical ``"{exchange}:{native_symbol}"``
        form produced by :class:`MarketStatsWorker`. Routes call this
        accessor and surface a placeholder envelope if the worker has
        not yet produced a value for a configured pair.

        Args:
            left_key: Canonical pair key for the left leg.
            right_key: Canonical pair key for the right leg.

        Returns:
            The most recent :class:`PairStats` snapshot, or ``None``
            when the pair is not (yet) in the stats map.
        """
        async with self._lock:
            return self._stats.get((left_key, right_key))

    async def set_pair_stats(self, left_key: str, right_key: str, stats: PairStats) -> None:
        """Atomic write of a :class:`PairStats` entry by canonical pair key.

        Args:
            left_key: Canonical pair key for the left leg.
            right_key: Canonical pair key for the right leg.
            stats: Updated :class:`PairStats` snapshot to install.
        """
        async with self._lock:
            self._stats[(left_key, right_key)] = stats

    async def pair_stats_keys(self) -> list[tuple[str, str]]:
        """Return a snapshot of all stat pair keys for diagnostic routes.

        Returns:
            Shallow copy of the ``_stats`` dict keys so callers can
            iterate without holding the cache lock.
        """
        async with self._lock:
            return list(self._stats.keys())

    async def _prewarm(self) -> None:
        """Backfill the cache from the DB for every persisted instrument.

        Iterates the policy's persisted set for the ``"candles"`` data
        type, expands wildcard entries (``"*"`` meaning "every symbol
        on this exchange") via :meth:`Repository.get_exchange_instruments`,
        then fetches up to ``_CACHE_CANDLE_LIMIT`` most-recent 1m rows
        per resolved instrument and seeds the deque in chronological
        order. Per-instrument failures are logged + skipped so one
        bad symbol cannot block warmup.

        Wildcard expansion was added 2026-05-24 after the live restart
        observation that operator-configured persist settings like
        ``market_persist_candles = {"mode":"explicit","exchanges":
        {"kraken":["*"]}}`` produced 0-instruments warm: the prior
        implementation called ``repository.get_candles(instrument="*")``
        verbatim and trivially matched zero rows. Wildcards now route
        through the per-exchange symbol enumeration.

        Symbols resolve to instrument public IDs ONCE per exchange
        instead of once per instrument: the per-instrument read used to
        re-derive, through ``get_candles``, an instrument this method had
        already enumerated, costing one pooled checkout plus a
        ``Symbol``-to-``Instrument`` join per symbol. Candle reads then
        run in slices of ``_PREWARM_INSTRUMENT_SLICE`` instruments per
        session, at most ``_PREWARM_MAX_CONCURRENCY`` slices at a time.

        Reads run in two passes. The first bounds ``open_at`` at
        ``_PREWARM_LOOKBACK``, which turns a backward index walk only
        ``LIMIT`` could terminate into a two-column range descent; the
        second re-reads only the instruments the first left short of
        ``_CACHE_CANDLE_LIMIT``, with NO floor. Dropping the floor can
        only add rows, and under ``ORDER BY open_at DESC LIMIT n`` the
        top-n of a superset contains the top-n of the floored set, so the
        second pass's result supersedes the first and final coverage is
        identical to an unfloored read. The floor is therefore a pure
        optimisation that cannot drop an instrument from the cache.

        Prewarm stays inline, ahead of the SUB socket, so
        :meth:`_apply_prewarm_rows` can assign deques wholesale without
        racing live ingest.

        The completion tally is derived from the BEST row count each
        instrument reached across both passes, never from the last pass
        to touch it. A second-pass read that raises leaves the first
        pass's rows installed in the deque, so reporting that instrument
        as ``failed`` would contradict the cache's actual contents; an
        instrument counts as ``failed`` only when it ends with no rows
        AND some pass raised for it.
        """
        started = asyncio.get_event_loop().time()
        as_of = datetime.now(UTC)
        targets, unresolved, skipped_exchanges = await self._collect_prewarm_targets(as_of)
        run = _PrewarmRun(
            targets=targets,
            as_of=as_of,
            semaphore=asyncio.Semaphore(_PREWARM_MAX_CONCURRENCY),
            budget=_PrewarmBudget(deadline=started + _PREWARM_DEADLINE_S),
            counts={},
            raised=set(),
        )
        for open_at_floor in (as_of - _PREWARM_LOOKBACK, None):
            if not await self._run_prewarm_pass(run, open_at_floor):
                break
        warmed = sum(1 for count in run.counts.values() if count)
        empty = sum(1 for key, count in run.counts.items() if not count and key not in run.raised)
        failed = sum(1 for key, count in run.counts.items() if not count and key in run.raised)
        elapsed = asyncio.get_event_loop().time() - started
        logger.info(
            "MarketCacheService prewarm complete: {} warmed, {} empty, {} unresolved, "
            "{} failed, {} exchanges skipped in {:.1f}s",
            warmed,
            empty,
            unresolved,
            failed,
            skipped_exchanges,
            elapsed,
        )
        if failed or skipped_exchanges:
            logger.warning(
                "MarketCacheService prewarm degraded: {} instruments failed, "
                "{} exchanges skipped",
                failed,
                skipped_exchanges,
            )

    async def _run_prewarm_pass(
        self,
        run: _PrewarmRun,
        open_at_floor: datetime | None,
    ) -> bool:
        """Run one floored or unbounded prewarm pass when work remains."""
        pending = [
            target
            for target in run.targets
            if run.counts.get((target.exchange, target.native_symbol), 0) < _CACHE_CANDLE_LIMIT
        ]
        if not pending or run.budget.exhausted(asyncio.get_event_loop().time()):
            return False
        slices = [
            pending[index : index + _PREWARM_INSTRUMENT_SLICE]
            for index in range(0, len(pending), _PREWARM_INSTRUMENT_SLICE)
        ]
        results = await asyncio.gather(
            *(
                self._prewarm_slice(
                    chunk,
                    run.as_of,
                    open_at_floor,
                    run.semaphore,
                    run.budget,
                )
                for chunk in slices
            )
        )
        for result in results:
            for key, (outcome, count) in result.items():
                run.counts[key] = max(run.counts.get(key, 0), count)
                if outcome == "failed":
                    run.raised.add(key)
        return True

    async def _collect_prewarm_targets(
        self, as_of: datetime
    ) -> tuple[list[_PrewarmTarget], int, int]:
        """Expand the persist set to resolved instruments, batched per exchange.

        Wildcard expansion keeps its own per-exchange guard so the
        2026-05-24 behaviour and its blast-radius ceiling are unchanged.
        Symbol resolution then runs ONCE per exchange through
        :meth:`Repository.get_instrument_public_ids_by_symbols`, which
        applies the identical ``where_active`` pair the per-instrument
        read used to apply one symbol at a time. Symbols absent from the
        returned mapping are alias-only rows with no active instrument;
        they are dropped before any candle read and warm zero rows,
        exactly as before.

        Args:
            as_of: Snapshot time threading the temporal queries.

        Returns:
            Tuple of the resolved targets, the count of symbols that did
            not resolve, and the count of exchanges skipped entirely.
        """
        by_exchange: dict[AllExchange, set[str]] = {}
        skipped_exchanges = 0
        for exchange, native_symbol in self.persist_policy.iter_persisted_instruments("candles"):
            if native_symbol == "*":
                try:
                    expanded = await self.repository.get_exchange_instruments(
                        exchange=str(exchange), as_of=as_of
                    )
                except Exception as exc:
                    logger.warning(
                        "MarketCacheService prewarm wildcard expansion failed for {}: {}",
                        exchange,
                        exc,
                    )
                    skipped_exchanges += 1
                    continue
            else:
                expanded = [native_symbol]
            by_exchange.setdefault(exchange, set()).update(expanded)
        targets: list[_PrewarmTarget] = []
        unresolved = 0
        for exchange, symbols in by_exchange.items():
            try:
                mapping = await self.repository.get_instrument_public_ids_by_symbols(
                    symbols, str(exchange), as_of
                )
            except Exception as exc:
                logger.warning(
                    "MarketCacheService prewarm instrument resolve failed for {}: {}",
                    exchange,
                    exc,
                )
                skipped_exchanges += 1
                continue
            unresolved += len(symbols) - len(mapping)
            targets.extend(
                _PrewarmTarget(exchange, symbol, instrument_public_id)
                for symbol, instrument_public_id in mapping.items()
            )
        return targets, unresolved, skipped_exchanges

    async def _apply_prewarm_rows(
        self, exchange: AllExchange, native_symbol: str, rows: Sequence[CandleRow]
    ) -> int:
        """Seed one instrument's deque from DESC-ordered DB rows.

        Wholesale assignment is safe because :meth:`start` awaits prewarm
        BEFORE creating the SUB socket, so no ingest task exists and no
        live bar can already be in the deque. If prewarm is ever moved
        off the lifespan critical path, this MUST become a merge that
        folds DB snaps UNDER existing live snaps by ``open_at_ms``: a
        live frame promoted by :meth:`_dispatch_candle` would otherwise
        be silently discarded when prewarm reaches that symbol.

        Args:
            exchange: Source exchange identifier.
            native_symbol: Native-format symbol.
            rows: Candle rows in DESCENDING ``open_at`` order.

        Returns:
            Number of snaps installed; ``0`` when ``rows`` is empty.
        """
        if not rows:
            return 0
        snaps = [_snap_from_candle_row(row) for row in reversed(rows)]
        key = (exchange, native_symbol)
        async with self._lock:
            self._candles[key] = deque(snaps, maxlen=_CACHE_CANDLE_LIMIT)
            self._last_seen_at[key] = asyncio.get_event_loop().time()
        return len(snaps)

    async def _prewarm_slice(
        self,
        targets: Sequence[_PrewarmTarget],
        as_of: datetime,
        open_at_floor: datetime | None,
        semaphore: asyncio.Semaphore,
        budget: _PrewarmBudget,
    ) -> dict[tuple[AllExchange, str], tuple[PrewarmOutcome, int]]:
        """Read and install one slice of instruments under the concurrency cap.

        The guard spans BOTH the repository call and the apply loop: an
        exception escaping here would propagate out of :meth:`_prewarm`
        and out of :meth:`start` while ``_listener_lock`` is held,
        aborting the lifespan and turning "slow to bind" into "never
        binds". A slice-level failure degrades to the per-instrument
        path so one bad symbol still cannot block warmup.

        That fallback is deliberately budgeted. It answers "one poisoned
        symbol", not "the database is gone": unbudgeted, an unreachable
        database would turn each failed batch read into
        ``_PREWARM_INSTRUMENT_SLICE`` further reads that each wait out
        the pool timeout, and with both passes retrying every
        zero-count target the phase would stall for hours behind an
        unbound port. The shared :class:`_PrewarmBudget` therefore fails
        the slice fast once failures look systemic or the deadline has
        passed.

        Args:
            targets: Instruments in this slice.
            as_of: Snapshot time threading the temporal query.
            open_at_floor: Inclusive ``open_at`` bound, or ``None``.
            semaphore: Bounds concurrent slices against the DB pool.
            budget: Shared deadline and slice-failure budget.

        Returns:
            Mapping from cache key to the outcome and installed count.
        """
        async with semaphore:
            outcomes: dict[tuple[AllExchange, str], tuple[PrewarmOutcome, int]] = {}
            if budget.exhausted(asyncio.get_event_loop().time()):
                for target in targets:
                    outcomes[(target.exchange, target.native_symbol)] = ("failed", 0)
                return outcomes
            try:
                by_public_id = await self.repository.get_latest_candles_for_instruments(
                    [target.instrument_public_id for target in targets],
                    _TARGET_TIMEFRAME,
                    as_of,
                    _CACHE_CANDLE_LIMIT,
                    open_at_floor,
                )
                for target in targets:
                    count = await self._apply_prewarm_rows(
                        target.exchange,
                        target.native_symbol,
                        by_public_id.get(target.instrument_public_id, []),
                    )
                    outcomes[(target.exchange, target.native_symbol)] = (
                        "warmed" if count else "empty",
                        count,
                    )
            except Exception as exc:
                budget.slice_failures += 1
                logger.warning(
                    "MarketCacheService prewarm slice failed for {} instruments on {} "
                    "({} pass), failure {} of {}: {}",
                    len(targets),
                    ", ".join(sorted({str(target.exchange) for target in targets})),
                    "floored" if open_at_floor is not None else "unfloored",
                    budget.slice_failures,
                    _PREWARM_SLICE_FAILURE_BUDGET,
                    exc,
                )
                for target in targets:
                    key = (target.exchange, target.native_symbol)
                    if budget.exhausted(asyncio.get_event_loop().time()):
                        outcomes[key] = ("failed", 0)
                        continue
                    outcomes[key] = await self._prewarm_one_fallback(target, as_of)
            return outcomes

    async def _prewarm_one_fallback(
        self, target: _PrewarmTarget, as_of: datetime
    ) -> tuple[PrewarmOutcome, int]:
        """Prewarm one instrument after its slice read failed.

        Preserves the pre-batch failure isolation and, critically, the
        per-symbol WARNING that is the only operator-visible record of
        which symbol failed. Passes no ``open_at_floor``: if the slice
        already failed, the floor is not worth also risking.

        Args:
            target: The instrument to read.
            as_of: Snapshot time threading the temporal query.

        Returns:
            The outcome and the installed row count.
        """
        try:
            by_public_id = await self.repository.get_latest_candles_for_instruments(
                [target.instrument_public_id],
                _TARGET_TIMEFRAME,
                as_of,
                _CACHE_CANDLE_LIMIT,
                None,
            )
        except Exception as exc:
            logger.warning(
                "MarketCacheService prewarm failed for {} {}: {}",
                target.exchange,
                target.native_symbol,
                exc,
            )
            return "failed", 0
        count = await self._apply_prewarm_rows(
            target.exchange,
            target.native_symbol,
            by_public_id.get(target.instrument_public_id, []),
        )
        return "warmed" if count else "empty", count

    async def _ingest_loop(self) -> None:
        """Consume market.* frames, filter to closed 1m candles, dispatch."""
        subscriber = self._subscriber
        if subscriber is None:
            return
        try:
            while self._running:
                frame = await self._recv_one_frame(subscriber)
                if frame is None:
                    continue
                topic, payload = frame
                if not self._should_ingest_topic(topic):
                    continue
                candle = self._parse_candle(payload)
                if candle is None:
                    continue
                if candle.timeframe != _TARGET_TIMEFRAME:
                    continue
                await self._dispatch_candle(candle)
        except asyncio.CancelledError:
            logger.info("MarketCacheService: ingest loop cancelled")
            raise

    async def _recv_one_frame(self, subscriber: ValidatedSubscriber) -> tuple[str, bytes] | None:
        """Receive one (topic, payload) frame; ``None`` on transient failure."""
        try:
            topic_bytes, payload_bytes = await subscriber.recv_multipart()
            topic = topic_bytes.decode() if isinstance(topic_bytes, bytes) else str(topic_bytes)
            payload = (
                payload_bytes if isinstance(payload_bytes, bytes) else str(payload_bytes).encode()
            )
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            logger.error("MarketCacheService listener recv failed: {}", exc)
            await asyncio.sleep(_LISTEN_RECV_BACKOFF_S)
            return None
        return topic, payload

    @staticmethod
    def _should_ingest_topic(topic: str) -> bool:
        """Cheap suffix gate before parsing the payload."""
        return topic.endswith(_TARGET_TOPIC_SUFFIX)

    @staticmethod
    def _parse_candle(payload: bytes) -> CandleData | None:
        """Parse a JSON payload into :class:`CandleData`; ``None`` on failure."""
        try:
            return CandleData.model_validate_json(payload)
        except Exception as exc:
            logger.debug("MarketCacheService: candle parse failed: {}", exc)
            return None

    async def _dispatch_candle(self, candle: CandleData) -> None:
        """Apply the closed-bar dedup state machine to ``candle``.

        State machine per ``(exchange, native_symbol)`` key:

        - No ``_current`` slot: seed it. Nothing appended to the deque.
        - Incoming ``open_at`` equals current: upsert (the live bar
          is still being built; replace OHLCV).
        - Incoming ``open_at`` greater than current: promote — append
          the prior current to the deque (now closed by construction),
          then set current to the new frame. When the deque's tail already
          holds that same minute the tail is REPLACED rather than appended:
          prewarm seeds the deque from the database, so a live frame for a
          minute prewarm already loaded would otherwise be promoted alongside
          it and the series would carry the same ``open_at`` twice. The live
          value wins, being the later observation of the same window.
        - Incoming ``open_at`` less than current: drop as stale; rare,
          debug-log only.
        """
        key = (candle.exchange, candle.instrument)
        snap = _snap_from_candle_data(candle)
        async with self._lock:
            self._last_seen_at[key] = asyncio.get_event_loop().time()
            current = self._current.get(key)
            if current is None:
                self._current[key] = snap
                return
            if snap.open_at_ms == current.open_at_ms:
                self._current[key] = snap
                return
            if snap.open_at_ms > current.open_at_ms:
                cached = self._candles.setdefault(key, deque(maxlen=_CACHE_CANDLE_LIMIT))
                if cached and cached[-1].open_at_ms == current.open_at_ms:
                    cached[-1] = current
                else:
                    cached.append(current)
                self._current[key] = snap
                return
            logger.debug(
                "MarketCacheService: stale candle for {} {} (open_at_ms={} < current={})",
                candle.exchange,
                candle.instrument,
                snap.open_at_ms,
                current.open_at_ms,
            )

    async def _prune_loop(self) -> None:
        """Walk ``_last_seen_at`` every minute and drop entries idle for 6h."""
        try:
            while self._running or self._prune_task is None:
                await asyncio.sleep(_PRUNE_LOOP_INTERVAL_S)
                await self._prune_once()
        except asyncio.CancelledError:
            logger.info("MarketCacheService: prune loop cancelled")
            raise

    async def _prune_once(self) -> None:
        """One pass of the stale-key prune."""
        cutoff = asyncio.get_event_loop().time() - _STALE_AFTER_S
        async with self._lock:
            stale = [key for key, last in self._last_seen_at.items() if last < cutoff]
            for key in stale:
                self._candles.pop(key, None)
                self._current.pop(key, None)
                self._last_seen_at.pop(key, None)
        if stale:
            logger.info("MarketCacheService prune: dropped {} stale instruments", len(stale))

    async def record_candle_for_test(self, candle: CandleData) -> None:
        """Test-only hook to push a candle through the dispatch path.

        Production code never calls this; the ingest loop owns frames
        in flight. Tests use this entry point to verify the dedup
        state machine without spinning up a real broker.

        Args:
            candle: Decoded :class:`CandleData` to feed through the
                :meth:`_dispatch_candle` state machine.
        """
        if candle.timeframe != _TARGET_TIMEFRAME:
            return
        await self._dispatch_candle(candle)

    @property
    def last_seen_for_test(self) -> dict[tuple[AllExchange, str], float]:
        """Test-only view of ``_last_seen_at`` (the loop-time stamps).

        Returns:
            Shallow copy of the internal map so tests can inspect
            without risk of mutating live state.
        """
        return dict(self._last_seen_at)

    def force_stale_for_test(self, key: tuple[AllExchange, str], age_s: float) -> None:
        """Test-only hook to age a key beyond the prune cutoff.

        Used by the prune loop test to simulate the 6-hour stale window
        without actually waiting.

        Args:
            key: ``(exchange, native_symbol)`` cache key to back-date.
            age_s: Number of seconds in the past to set the
                ``_last_seen_at`` stamp.
        """
        self._last_seen_at[key] = asyncio.get_event_loop().time() - age_s

    def staleness_window_seconds(self) -> int:
        """Return the configured stale-after window for diagnostics.

        Returns:
            Configured :data:`_STALE_AFTER_S` value in seconds.
        """
        return _STALE_AFTER_S

    def cache_capacity_per_instrument(self) -> int:
        """Return the per-instrument deque cap for diagnostics.

        Returns:
            Configured :data:`_CACHE_CANDLE_LIMIT` value.
        """
        return _CACHE_CANDLE_LIMIT


def _format_stale_age(seconds: float) -> str:
    """Render a stale age as ``H:MM:SS`` for log messages."""
    return str(timedelta(seconds=int(seconds)))
