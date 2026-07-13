"""Abstract base class for exchange client implementations.

This module defines the ExchangeClientBase abstract class that serves as
the contract for all exchange client implementations. It provides:

- Async context manager support for connection lifecycle
- Abstract methods for market data retrieval (tickers, OHLCV)
- Abstract methods for order management (create, cancel, get)
- Abstract methods for real-time data subscriptions via WebSocket
- Internal methods for logging orders and executions to database

All exchange implementations (Kraken, Walutomat, Paper, Polygon)
must inherit from this base class and implement its abstract methods.
"""

import asyncio
import concurrent.futures
import contextlib
import contextvars
import functools
import time
from abc import ABC
from abc import abstractmethod
from collections import Counter
from collections.abc import AsyncIterator
from collections.abc import Callable
from datetime import UTC
from datetime import datetime
from types import TracebackType
from typing import Any
from typing import Self

from loguru import logger
from sqlalchemy.exc import SQLAlchemyError

from snapper.core.types import ExchangeEnum
from snapper.core.types import ExecutionModeEnum
from snapper.data.repository import Repository
from snapper.infrastructure.exchanges._subscription_health import SubscriptionHealthTracker
from snapper.infrastructure.exchanges._subscription_health import _SymbolEntry
from snapper.infrastructure.exchanges.contracts import EXCHANGE_TO_CORE_ORDER_TYPE
from snapper.infrastructure.exchanges.contracts import AccountBalance
from snapper.infrastructure.exchanges.contracts import CandleUpdate
from snapper.infrastructure.exchanges.contracts import CapabilityStatus
from snapper.infrastructure.exchanges.contracts import ExchangeOrderRequest
from snapper.infrastructure.exchanges.contracts import ExchangeOrderSnapshot
from snapper.infrastructure.exchanges.contracts import ExchangeOrderStatusEnum
from snapper.infrastructure.exchanges.contracts import ExecutionUpdate
from snapper.infrastructure.exchanges.contracts import NativeBalanceEntry
from snapper.infrastructure.exchanges.contracts import OhlcvSnapshot
from snapper.infrastructure.exchanges.contracts import OpenPositionSnapshot
from snapper.infrastructure.exchanges.contracts import OrderFillSummary
from snapper.infrastructure.exchanges.contracts import TickerSnapshot
from snapper.infrastructure.exchanges.contracts import TickerUpdate
from snapper.infrastructure.exchanges.contracts import TradeUpdate
from snapper.infrastructure.exchanges.contracts import to_fill_status
from snapper.infrastructure.exchanges.errors import RestPoolDispatchError
from snapper.infrastructure.rest.tracker import get_rest_call_tracker
from snapper.infrastructure.symbols.functions import resolve_symbol_public_id
from snapper.messaging.infrastructure.publisher import SequenceTracker

__all__ = ["ExchangeClientBase"]

_REST_POOL_MAX_WORKERS = 4
"""Per-client cap on concurrent blocking REST threads.

Sized for the realistic concurrency of one (venue, wallet) client —
submit + cancel + reconciliation verification + balance/funding probe.
Do not shrink: a cancelled await does not stop an already-running SDK
call, so even sequential caller code can temporarily occupy multiple
workers. Saturation queues inside the pool (FIFO), which is the
desired backpressure: a dead venue consumes at most this many threads
of ITS OWN pool and none of any other client's or of the event loop's
shared default executor.
"""

_NATIVE_BALANCES_UNSUPPORTED_MSG = "This exchange client does not support native balance reads"
_NATIVE_POSITIONS_UNSUPPORTED_MSG = "This exchange client does not support native position reads"


class ExchangeClientBase(ABC):
    """Abstract base class defining the interface for exchange clients.

    This class provides a standardized interface for interacting with
    cryptocurrency and FX exchanges. Implementations must provide both
    REST API methods for data retrieval and order management, as well
    as WebSocket subscriptions for real-time market data.

    Attributes:
        supports_websocket_executions: Whether the exchange supports
            real-time execution updates via WebSocket.
        repository: Optional database repository for order/execution logging.
        exchange_name: Name identifier for the exchange.
    """

    supports_websocket_executions: bool = True
    balance_capability: CapabilityStatus = CapabilityStatus.UNSUPPORTED
    position_capability: CapabilityStatus = CapabilityStatus.UNSUPPORTED

    def __init__(
        self, repository: Repository | None = None, exchange_name: str = "unknown"
    ) -> None:
        """Initialize the exchange client base.

        Args:
            repository: Optional database repository for persisting orders
                and executions. If None, database logging is disabled.
            exchange_name: Identifier for the exchange (e.g., "kraken", "walutomat").
        """
        self.repository = repository
        self.exchange_name = exchange_name
        self._tracker: SequenceTracker | None = None
        self._health_tracker: SubscriptionHealthTracker | None = None
        self._health_loop_running: bool = False
        self._health_loop_task: asyncio.Task[None] | None = None
        self._rest_pool: concurrent.futures.ThreadPoolExecutor | None = None
        self._rest_pool_closed: bool = False

    def set_tracker(self, tracker: SequenceTracker) -> None:
        """Inject the component-level SequenceTracker for provenance stamping.

        Called once at executor start so that order and execution DB writes
        share the same session_id and counter owner as the ZMQ publisher.

        Args:
            tracker: SequenceTracker owned by the parent executor component.
        """
        self._tracker = tracker

    def _record_rest_call(self) -> None:
        """Report one outgoing REST call to the process-scoped tracker.

        Called by each implementation's retry/request wrapper so every
        retry attempt counts as one call (the tracker is observability,
        not budgeting — retries do consume the upstream budget and we
        want the metric to reflect that). Safe to call from any async
        context; the tracker is thread-safe.
        """
        get_rest_call_tracker().record_call(self.exchange_name)

    async def _acquire_rest_slot(self) -> None:
        """Pre-emptively wait for REST budget before the next call.

        Delegates to ``RestCallTracker.acquire`` which serialises
        capacity checks per exchange and sleeps until the rolling 1 s
        window dips below the published limit. Exchanges without a
        published limit pass through immediately (the tracker has no
        ground truth to pre-empt against; the existing
        ``_with_retry`` / ccxt handlers still catch upstream 429s).
        """
        await get_rest_call_tracker().acquire(self.exchange_name)

    def _ensure_rest_pool(self) -> concurrent.futures.ThreadPoolExecutor:
        """Return this client's bounded REST thread pool, creating it lazily.

        Returns:
            The client-owned executor for blocking REST dispatch.

        Raises:
            RuntimeError: When the pool has been shut down and not
                reopened via ``connect()`` — a post-disconnect REST call
                indicates a lifecycle bug and must fail loudly instead
                of resurrecting a pool nobody will clean up.
        """
        if self._rest_pool_closed:
            raise RuntimeError(f"{self.exchange_name}: REST thread pool is closed")
        pool = self._rest_pool
        if pool is None:
            pool = concurrent.futures.ThreadPoolExecutor(
                max_workers=_REST_POOL_MAX_WORKERS,
                thread_name_prefix=f"{self.exchange_name}-rest",
            )
            self._rest_pool = pool
        return pool

    async def _dispatch_blocking[**P, R](
        self, func: Callable[P, R], /, *args: P.args, **kwargs: P.kwargs
    ) -> R:
        """Run a blocking (synchronous) REST callable on the client's own pool.

        Replaces ``asyncio.to_thread`` for venue REST so that dead-network
        stalls saturate at most this client's bounded pool instead of the
        loop's shared default executor (audit P1-5). Contextvars are
        propagated via ``copy_context`` for full ``asyncio.to_thread``
        parity: the Polygon client logs inside its worker-thread
        callables and the log-context ``ContextVar`` is read by record
        formatting there. Cancelling the await abandons the result but
        the thread keeps running — same semantics the default-executor
        dispatch had.

        Args:
            func: Synchronous callable to execute (never a coroutine
                function — async callables are awaited by callers directly).
            *args: Positional arguments for ``func``.
            **kwargs: Keyword arguments for ``func``.

        Returns:
            The callable's result.

        Raises:
            RuntimeError: When the pool is closed (see
                ``_ensure_rest_pool``) or the stdlib refuses the submit
                pre-enqueue during a shutdown race — both mean the
                callable was definitively NOT sent.
            RestPoolDispatchError: When scheduling fails AFTER the work
                item may have been enqueued (``ThreadPoolExecutor.submit``
                enqueues before spawning a worker, so e.g. "can't start
                new thread" leaves the callable queued and it may still
                run on a freed worker) — submit paths classify this as
                ambiguous, never as a definitive reject.
        """
        loop = asyncio.get_running_loop()
        pool = self._ensure_rest_pool()
        ctx = contextvars.copy_context()
        call = functools.partial(ctx.run, functools.partial(func, *args, **kwargs))
        try:
            future = loop.run_in_executor(pool, call)
        except RuntimeError as exc:
            if "after shutdown" in str(exc) or "after interpreter shutdown" in str(exc):
                raise
            raise RestPoolDispatchError(str(exc)) from exc
        return await future

    def _reopen_rest_pool(self) -> None:
        """Re-enable blocking REST dispatch on this client instance.

        Called as the FIRST step of each implementation's ``connect()``
        so same-instance reconnect cycles (publisher rebuilds) get a
        fresh lazily-created pool after a prior ``disconnect()``.
        """
        self._rest_pool_closed = False

    def _shutdown_rest_pool(self) -> None:
        """Shut down the client's REST pool without waiting for stragglers.

        Idempotent. Uses ``shutdown(wait=False, cancel_futures=True)`` so
        queued-but-not-started work is dropped and the interpreter's
        atexit join only ever waits on calls already in flight (bounded
        by their HTTP timeouts). Clears the pool slot so a later
        ``connect()`` builds a fresh pool rather than touching a
        shut-down executor.
        """
        self._rest_pool_closed = True
        pool = self._rest_pool
        self._rest_pool = None
        if pool is not None:
            pool.shutdown(wait=False, cancel_futures=True)

    def subscription_health_snapshot(self) -> dict[tuple[str, str], _SymbolEntry]:
        """Return a point-in-time copy of subscription-health state.

        Returns the owned :class:`SubscriptionHealthTracker` snapshot
        when this client tracks per-symbol subscription health, else an
        empty mapping. Polling-only sources and clients that never
        instantiate a tracker have no per-symbol identity to report and
        return ``{}`` so callers (e.g. the publisher feed-health flush)
        can iterate uniformly without a ``None`` guard.

        Args:
            None.

        Returns:
            Mapping from ``(channel, symbol)`` to copied
            :class:`_SymbolEntry` objects, or an empty mapping when no
            tracker exists.

        Raises:
            None.
        """
        tracker = self._health_tracker
        if tracker is None:
            return {}
        return tracker.snapshot()

    def start_health_loop(self) -> None:
        """Start the subscription health retry loop when a tracker exists.

        Args:
            None.

        Returns:
            None.

        Raises:
            None.
        """
        if self._health_tracker is None or self._health_loop_task is not None:
            return
        self._health_loop_running = True
        self._health_loop_task = asyncio.create_task(self._subscription_health_loop())

    async def stop_health_loop(self) -> None:
        """Stop the subscription health retry loop if it is running.

        Args:
            None.

        Returns:
            None.

        Raises:
            None.
        """
        self._health_loop_running = False
        task = self._health_loop_task
        if task is None:
            return
        task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await task
        self._health_loop_task = None

    async def _subscription_health_loop(self) -> None:
        """Retry overdue pending subscribes and log stale data.

        Stale-subscription logging runs FIRST in each cycle: it is read-only
        and must never be hostage to send behavior. In the 2026-06-09
        blackout incident a retry pass blocked forever inside a venue send,
        and because logging ran after the retries the publisher emitted zero
        stale warnings for its entire connected-but-dark lifetime — the one
        signal an operator could have alerted on was muted by the very
        failure it should have exposed.

        Args:
            None.

        Returns:
            None.

        Raises:
            asyncio.CancelledError: Propagated when the lifecycle stop
                method cancels the background task.
        """
        tracker = self._health_tracker
        if tracker is None:
            return
        while self._health_loop_running:
            await asyncio.sleep(tracker.retry_interval_s)
            self._log_stale_subscriptions(tracker)
            await self._retry_overdue_pending_subscriptions(tracker)
            await self._retry_due_failed_subscriptions(tracker)
            await self._recover_dark_subscriptions(tracker)

    async def _retry_overdue_pending_subscriptions(
        self, tracker: SubscriptionHealthTracker
    ) -> None:
        """Retry pending subscriptions whose ACK timer expired.

        Each overdue entry consumes one fast retry attempt, guarded by the
        ``expected`` identity check so a concurrent reconnect replay that
        replaced the entry is not stale-retried. A genuine fast-budget
        exhaustion is logged via :meth:`_log_pending_backoff`; an entry the
        concurrent reader confirmed or rejected mid-loop is skipped
        silently so a healthy recovery never produces a false failure alert.

        Args:
            tracker: Health tracker that owns pending subscription state.

        Returns:
            None.

        Raises:
            None.
        """
        for entry in tracker.list_overdue_pending():
            if tracker.mark_retry_attempt(entry.channel, entry.symbol, expected=entry):
                await self._rate_limited_retry_subscribe(tracker, entry.channel, entry.symbol)
            else:
                self._log_pending_backoff(entry)

    def _log_pending_backoff(self, entry: _SymbolEntry) -> None:
        """Log a fast-budget exhaustion, skipping mid-loop recoveries.

        Only an entry this attempt itself drove to a scheduled ``failed``
        state is logged: a first exhaustion at WARNING, a re-failure after a
        slow retry at DEBUG. An entry that recovered (now confirmed) or was
        explicitly rejected (failed but unscheduled) mid-loop stays silent.

        First exhaustion is WARNING, not ERROR, because a single symbol
        backing off is recoverable per-symbol degradation, not a venue
        outage: the slow-retry and dark-recovery passes keep nudging it,
        the venue-wide signal already surfaces as the aggregated stale
        WARNING in :meth:`_log_stale_subscriptions`, and a genuinely dark
        venue is escalated to ERROR with a reconnect by the publisher
        liveness watchdog. Reserving ERROR for those venue-level paths
        keeps the operator alert tier from being flooded by benign
        per-symbol churn (illiquid contracts, closed-market resubscribes).

        Args:
            entry: The overdue entry whose retry attempt returned False.

        Returns:
            None.

        Raises:
            None.
        """
        if entry.status != "failed" or entry.next_attempt_at is None:
            return
        if entry.slow_retry_count == 0:
            logger.warning(
                "{}: subscribe failed for {}/{} after {} retries; backing off",
                self.exchange_name,
                entry.channel,
                entry.symbol,
                entry.retry_count,
            )
        else:
            logger.debug(
                "{}: {}/{} still unconfirmed after slow retry {}; backing off",
                self.exchange_name,
                entry.channel,
                entry.symbol,
                entry.slow_retry_count,
            )

    async def _retry_due_failed_subscriptions(self, tracker: SubscriptionHealthTracker) -> None:
        """Reissue failed subscriptions whose slow-retry backoff elapsed.

        Each gets one more ACK window; these slow retries and any
        subsequent re-failures log at DEBUG so a permanently unsupported
        subscription self-heals if it ever becomes available without
        spamming ERROR forever.

        Args:
            tracker: Health tracker that owns failed subscription state.

        Returns:
            None.

        Raises:
            None.
        """
        for entry in tracker.list_due_failed():
            if not tracker.mark_slow_retry(entry.channel, entry.symbol):
                continue
            logger.debug(
                "{}: slow-retrying {}/{} (attempt {})",
                self.exchange_name,
                entry.channel,
                entry.symbol,
                entry.slow_retry_count,
            )
            await self._rate_limited_retry_subscribe(tracker, entry.channel, entry.symbol)

    async def _recover_dark_subscriptions(self, tracker: SubscriptionHealthTracker) -> None:
        """Re-subscribe confirmed subscriptions that have gone dark.

        A confirmed channel that has delivered no data for far longer than
        the stale threshold (per
        :meth:`SubscriptionHealthTracker.list_due_dark_recovery`) is
        re-subscribed per-symbol to nudge the exchange into resuming the
        stream. Wildcard-seeded entries are excluded by the tracker, and the
        ``expected`` identity guard skips an entry a concurrent reader
        changed mid-loop.

        Args:
            tracker: Health tracker that owns confirmed subscription state.

        Returns:
            None.

        Raises:
            None.
        """
        for entry in tracker.list_due_dark_recovery():
            if not tracker.mark_dark_recovery(entry.channel, entry.symbol, expected=entry):
                continue
            logger.info(
                "{}: dark-recovery re-subscribe {}/{} (attempt {})",
                self.exchange_name,
                entry.channel,
                entry.symbol,
                entry.dark_recovery_count,
            )
            await self._rate_limited_retry_subscribe(tracker, entry.channel, entry.symbol)

    async def _rate_limited_retry_subscribe(
        self, tracker: SubscriptionHealthTracker, channel: str, symbol: str
    ) -> None:
        """Issue one re-subscribe, exception-guarded and rate-spaced.

        Shared by the pending, slow-failed and dark-recovery passes. A
        subscribe exception is caught and logged WARNING (a transient send
        failure must not abort the sweep), then the loop sleeps
        ``retry_subscribe_spacing_s`` so consecutive re-subscribes stay
        under the exchange per-connection subscribe message-rate limit
        (the cause of the observed "Exceeded msg rate" storms).

        Args:
            tracker: Health tracker supplying the spacing interval.
            channel: Tracker channel key.
            symbol: Wire-format symbol or product id.

        Returns:
            None.

        Raises:
            None.
        """
        try:
            await self._retry_subscribe(channel, symbol)
        except Exception as exc:
            logger.warning(
                "{}: retry subscribe raised for {}/{}: {}",
                self.exchange_name,
                channel,
                symbol,
                exc,
            )
        if tracker.retry_subscribe_spacing_s > 0:
            await asyncio.sleep(tracker.retry_subscribe_spacing_s)

    def _log_stale_subscriptions(self, tracker: SubscriptionHealthTracker) -> None:
        """Emit aggregate and per-entry diagnostics for stale data.

        Args:
            tracker: Health tracker that owns confirmed subscription state.

        Returns:
            None.

        Raises:
            None.
        """
        stale_entries = tracker.list_stale_data()
        if not stale_entries:
            return
        now = time.monotonic()
        by_channel: Counter[str] = Counter(entry.channel for entry in stale_entries)
        worst = self._worst_stale_entry(stale_entries, now)
        channel_summary = ", ".join(
            f"{channel}={count}" for channel, count in sorted(by_channel.items())
        )
        logger.warning(
            "{}: {} stale subscription(s) [{}]; worst: {}/{} for {:.0f}s",
            self.exchange_name,
            len(stale_entries),
            channel_summary,
            worst.channel,
            worst.symbol,
            self._entry_stale_age(worst, now),
        )
        for entry in stale_entries:
            logger.debug(
                "{}: subscribed to {}/{} but no data for {:.0f}s",
                self.exchange_name,
                entry.channel,
                entry.symbol,
                self._entry_stale_age(entry, now),
            )

    @staticmethod
    def _entry_age_ref(entry: _SymbolEntry) -> float:
        """Return the timestamp used to measure stale subscription age.

        Delegates to :meth:`_SymbolEntry.stale_reference` so the reported
        stale age uses the same reference as the stale decision in
        :meth:`SubscriptionHealthTracker.list_stale_data` (the most recent
        of request / confirmation / last-data). Without this a re-confirmed
        subscription would be flagged stale from the fresh reference yet
        reported with an exaggerated age anchored on the old data.

        Args:
            entry: Subscription entry to inspect.

        Returns:
            The entry's stale reference timestamp (monotonic seconds).
        """
        return entry.stale_reference()

    @classmethod
    def _entry_stale_age(cls, entry: _SymbolEntry, now: float) -> float:
        """Return the stale age for one subscription entry.

        Args:
            entry: Subscription entry to inspect.
            now: Current monotonic timestamp.

        Returns:
            Seconds elapsed since the entry's data reference timestamp.
        """
        return now - cls._entry_age_ref(entry)

    @classmethod
    def _worst_stale_entry(cls, entries: list[_SymbolEntry], now: float) -> _SymbolEntry:
        """Return the stale entry with the largest age.

        Args:
            entries: Non-empty stale subscription entries.
            now: Current monotonic timestamp.

        Returns:
            Stale entry with the oldest data reference timestamp.
        """
        worst = entries[0]
        worst_age = cls._entry_stale_age(worst, now)
        for entry in entries[1:]:
            entry_age = cls._entry_stale_age(entry, now)
            if entry_age > worst_age:
                worst = entry
                worst_age = entry_age
        return worst

    async def _retry_subscribe(self, channel: str, symbol: str) -> None:
        """Retry one symbol subscription.

        Args:
            channel: Tracker channel key to retry.
            symbol: Wire-format symbol or product id to retry.

        Returns:
            None.

        Raises:
            NotImplementedError: Always on the base class.
        """
        raise NotImplementedError(f"{type(self).__name__} must implement _retry_subscribe")

    @abstractmethod
    async def connect(self) -> None:
        """Establish connection to the exchange.

        This method should initialize any HTTP clients, WebSocket connections,
        and authenticate with the exchange if credentials are provided.

        Raises:
            RuntimeError: If connection fails.
        """
        ...

    @abstractmethod
    async def disconnect(self) -> None:
        """Gracefully close all connections to the exchange.

        This method should close WebSocket connections, HTTP sessions,
        and release any other resources.
        """
        ...

    async def __aenter__(self) -> Self:
        """Async context manager entry point.

        A failed or cancelled ``connect()`` triggers a best-effort
        ``disconnect()`` before re-raising: ``__aexit__`` never runs
        when ``__aenter__`` raises, and no caller reliably stops a
        process whose start died inside connect — without this, the
        client's REST session (created eagerly in ``__init__``) leaks
        on every failed start/respawn cycle. Cleanup errors are
        suppressed so the ORIGINAL connect failure always propagates.

        Returns:
            Self: The connected exchange client instance.
        """
        try:
            await self.connect()
        except BaseException:
            with contextlib.suppress(Exception):
                await self.disconnect()
            raise
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: TracebackType | None,
    ) -> None:
        """Async context manager exit point.

        Args:
            exc_type: Exception type if an exception was raised.
            exc_val: Exception instance if an exception was raised.
            exc_tb: Traceback if an exception was raised.
        """
        await self.disconnect()

    @abstractmethod
    async def get_ticker(self, symbol: str) -> TickerSnapshot:
        """Fetch current ticker data for a symbol.

        Args:
            symbol: Trading pair symbol in native format (e.g., "BTC/USD").

        Returns:
            TickerSnapshot with current bid, ask, last price, and volume.

        Raises:
            ValueError: If symbol is not supported.
            RuntimeError: If API request fails.
        """
        ...

    @abstractmethod
    async def get_ohlcv(
        self,
        symbol: str,
        timeframe: str = "1m",
        since: int | None = None,
        limit: int | None = None,
    ) -> list[OhlcvSnapshot]:
        """Fetch OHLCV (candlestick) data for a symbol.

        Args:
            symbol: Trading pair symbol in native format.
            timeframe: Candle interval (e.g., "1m", "5m", "1h", "1d").
            since: Start timestamp in milliseconds. If None, fetches recent data.
            limit: Maximum number of candles to return.

        Returns:
            List of OhlcvSnapshot objects ordered by timestamp ascending.

        Raises:
            ValueError: If symbol or timeframe is not supported.
            RuntimeError: If API request fails.
        """
        ...

    @abstractmethod
    async def create_order(self, request: ExchangeOrderRequest) -> ExchangeOrderSnapshot:
        """Submit a new order to the exchange.

        Args:
            request: Order parameters including symbol, side, type, amount, price.

        Returns:
            ExchangeOrderSnapshot with the created order details.

        Raises:
            RuntimeError: If API credentials are not configured or order fails.
            ValueError: If order parameters are invalid.
        """
        ...

    @abstractmethod
    async def cancel_order(self, order_id: str, symbol: str | None = None) -> ExchangeOrderSnapshot:
        """Cancel an existing order.

        Args:
            order_id: Exchange order ID to cancel.
            symbol: Optional symbol (required by some exchanges).

        Returns:
            ExchangeOrderSnapshot with the canceled order status.

        Raises:
            RuntimeError: If API credentials are not configured or cancel fails.
            ValueError: If order is not found.
        """
        ...

    @abstractmethod
    async def get_order(self, order_id: str, symbol: str | None = None) -> ExchangeOrderSnapshot:
        """Fetch details of a specific order.

        Args:
            order_id: Exchange order ID to retrieve.
            symbol: Optional symbol (required by some exchanges).

        Returns:
            ExchangeOrderSnapshot with current order status and fills.

        Raises:
            RuntimeError: If API credentials are not configured.
            ValueError: If order is not found.
        """
        ...

    async def find_order_by_client_id(
        self, client_order_id: str, symbol: str | None = None
    ) -> ExchangeOrderSnapshot | None:
        """Verify whether an order with the given client id exists on the venue.

        The resolver behind the ambiguous-submit UNKNOWN state: after
        a submit whose outcome is unknown, the executor asks the venue
        for the truth before deciding the order's fate.

        CONTRACT — ``None`` is an AUTHORITATIVE answer: it may be
        returned only when the venue was queried successfully across
        the full order universe (open AND closed/terminal) and the
        client id is absent. Any inability to verify — transport
        failure, partial query, or a venue without client-id lookup —
        must RAISE instead (``NotImplementedError`` for the latter), so
        the caller never converts "could not check" into "venue says
        no" and rejects a live order.

        Args:
            client_order_id: Client order id the submit was sent with.
            symbol: Optional native symbol to narrow the query.

        Returns:
            The order snapshot when found; None when authoritatively
            absent.

        Raises:
            NotImplementedError: If this venue cannot look up orders by
                client id (default implementation).
        """
        raise NotImplementedError(
            f"{self.exchange_name}: cannot authoritatively verify orders by client id"
        )

    @abstractmethod
    async def get_orders(
        self,
        symbol: str | None = None,
        status: ExchangeOrderStatusEnum | None = None,
        limit: int | None = None,
    ) -> list[ExchangeOrderSnapshot]:
        """Fetch multiple orders with optional filters.

        Args:
            symbol: Filter by trading pair. If None, returns all symbols.
            status: Filter by order status (OPEN, CLOSED, CANCELED, etc.).
            limit: Maximum number of orders to return.

        Returns:
            List of ExchangeOrderSnapshot objects matching the filters.

        Raises:
            RuntimeError: If API credentials are not configured.
        """
        ...

    supports_fill_summary: bool = False
    """Whether :meth:`get_order_fill_summary` has a real venue source.

    The fill-gap reconciler must distinguish "this venue has no
    per-order fills lookup" (honest no-source — a corrective may emit
    fee-less) from "the implemented source returned no usable data YET"
    (fills page lag — the corrective must DEFER, because its stable
    exec id would freeze a fee-less emission forever). Implementations
    overriding the summary set this True.
    """

    async def get_order_fill_summary(self, order_id: str) -> OrderFillSummary | None:
        """Return a venue-true price/fee aggregate over an order's own fills.

        Fill-gap reconciliation's last-resort price source AND its fee
        source (#145 P2-5): when an order snapshot carries no price (a
        market order whose venue payload also lacks an executed average),
        the executor asks the venue for the order's OWN fills and uses
        their quantity-weighted average price — venue truth, never a local
        approximation (the executor deliberately has no tick feed; see the
        fill-gap rationale in the executor base). The covered quantity is
        returned alongside because venues typically page their fills: a
        VWAP computed over a PARTIAL page must never be trusted as the
        order's average, so the caller compares the coverage against the
        order's total filled quantity and falls back to the skip when the
        page does not cover it. ``fee_total``/``fee_currency`` carry the
        summed fills fee when the venue reports one consistently —
        corrective fills then stop fabricating fee=0.

        The default returns ``None``: a venue without a usable per-order
        fills lookup keeps reconciliation's documented fail-safe skip. The
        zero-delay sleep keeps this default a genuine coroutine — venue
        overrides await real I/O and the reconciliation caller awaits
        through the base type, so the ``async`` signature must stay.

        Args:
            order_id: Exchange order ID whose fills should be aggregated.

        Returns:
            The fills aggregate, or ``None`` when no usable fills exist.

        Raises:
            Exception: Implementations may raise on venue/transport errors —
                the reconciliation caller treats any failure as
                could-not-resolve and skips.
        """
        await asyncio.sleep(0)
        return None

    async def read_native_balances(self) -> list[NativeBalanceEntry]:
        """Read faithful native per-currency balances for the account observer.

        The account observer calls this ONLY when ``balance_capability`` is not
        ``UNSUPPORTED``; the default therefore fail-closes by raising, so a
        client that advertises balance capability without a faithful reader is a
        loud bug, never a silent empty account. Unlike ``get_balance`` (consumed
        by the order-reconciliation plane, which may aggregate across
        currencies), this reader must return ONLY venue-reported native values —
        ``free``/``used`` NULL where the venue exposes no faithful split — or
        raise; it must never fabricate a split.

        Returns:
            Faithful native per-currency balance entries.

        Raises:
            NotImplementedError: When the client declares no balance capability.
        """
        await asyncio.sleep(0)
        raise NotImplementedError(_NATIVE_BALANCES_UNSUPPORTED_MSG)

    async def read_native_positions(self) -> list[OpenPositionSnapshot]:
        """Read faithful native open positions for the account observer.

        Called ONLY when ``position_capability`` is ``SUPPORTED``; the default
        fail-closes by raising. Implementations must STRICTLY validate the venue
        envelope — reject a missing positions collection, missing/unknown side,
        an unresolvable symbol, or any non-finite number — rather than coerce
        them into a zero/default position that would read as authoritative.

        Returns:
            Faithful open-position snapshots.

        Raises:
            NotImplementedError: When the client declares no position capability.
        """
        await asyncio.sleep(0)
        raise NotImplementedError(_NATIVE_POSITIONS_UNSUPPORTED_MSG)

    @abstractmethod
    async def get_balance(self, currency: str | None = None) -> dict[str, AccountBalance]:
        """Fetch account balances.

        Args:
            currency: Filter by specific currency. If None, returns all balances.

        Returns:
            Dictionary mapping currency codes to AccountBalance objects.

        Raises:
            RuntimeError: If API credentials are not configured.
        """
        ...

    @abstractmethod
    def subscribe_ticks(self, symbols: list[str]) -> AsyncIterator[TickerUpdate]:
        """Subscribe to real-time ticker updates.

        Args:
            symbols: List of symbols to subscribe. Use ["*"] for all symbols.

        Yields:
            TickerUpdate objects as they arrive from the exchange.

        Raises:
            RuntimeError: If WebSocket connection is not established.
        """
        ...

    @abstractmethod
    def subscribe_candles(
        self,
        symbols: list[str],
        timeframe: str = "1m",
    ) -> AsyncIterator[CandleUpdate]:
        """Subscribe to real-time candlestick updates.

        Args:
            symbols: List of symbols to subscribe. Use ["*"] for all symbols.
            timeframe: Candle interval (e.g., "1m", "5m", "1h").

        Yields:
            CandleUpdate objects as candles complete or update.

        Raises:
            RuntimeError: If WebSocket connection is not established.
        """
        ...

    @abstractmethod
    def subscribe_trades(self, symbols: list[str]) -> AsyncIterator[TradeUpdate]:
        """Subscribe to real-time trade updates.

        Args:
            symbols: List of symbols to subscribe. Use ["*"] for all symbols.

        Yields:
            TradeUpdate objects for each executed trade on the exchange.

        Raises:
            RuntimeError: If WebSocket connection is not established.
        """
        ...

    @abstractmethod
    def subscribe_executions(self) -> AsyncIterator[ExecutionUpdate]:
        """Subscribe to real-time execution updates for user's orders.

        This subscription requires API credentials and provides updates
        when the user's orders are filled, partially filled, or canceled.

        Yields:
            ExecutionUpdate objects for each execution event.

        Raises:
            RuntimeError: If API credentials are not configured or
                WebSocket connection is not established.
        """
        ...

    @abstractmethod
    def subscribe_instruments(self, **kwargs: Any) -> AsyncIterator[dict[str, Any]]:
        """Subscribe to instrument/market information updates.

        This subscription provides information about available trading pairs,
        their specifications, and status updates.

        Args:
            **kwargs: Exchange-specific subscription parameters.

        Yields:
            Dictionary containing instrument information.

        Raises:
            NotImplementedError: If exchange does not support this feature.
        """
        ...

    async def _log_order_to_db(
        self,
        request: ExchangeOrderRequest,
        order: ExchangeOrderSnapshot,
    ) -> tuple[int, str] | None:
        """Persist a new order to the database.

        This internal method is called after successfully creating an order
        on the exchange. It upserts the instrument and inserts the order record.

        This is a post-accept AUXILIARY write and must NEVER raise: at
        this point the venue already accepted the order, and an escaping
        exception would ride the venue client's create_order back into
        the executor's definitive-reject branch — misreporting a LIVE
        order as rejected (exactly the false-reject failure the
        ambiguous-submit UNKNOWN state exists to prevent). Hence the
        blanket exception catch; the caller treats None as "not
        persisted" and downstream reconciliation heals the row.

        ``order_type`` is persisted from ``request.type`` (normalized
        wire -> CORE, #156), not the snapshot: the request carries the
        original intent on every call site, while fetched ccxt snapshots
        collapse Kraken stop types to their unified base ``market`` /
        ``limit`` — logging the snapshot type on the adoption-repair
        path would durably misrepresent a protective stop as a plain
        order.

        ``mode`` is derived from the client's ``exchange_name`` (paper
        venue ⇒ ``paper``, anything else ⇒ ``live``), mirroring the
        engine rule in
        :meth:`snapper.application.engine.service.TradingEngineService.mode`.
        Before this was passed explicitly, the repository defaulted every
        omitted mode to ``live``, durably mislabeling paper orders — and
        mode participates in the active-order uniqueness index and in
        fill-gap recovery matching, so the label is operational, not
        cosmetic.

        Args:
            request: Original order request with parameters.
            order: Exchange response with order details.

        Returns:
            Tuple of (order_id, public_id) if successful, None if repository
            is not configured or operation fails.
        """
        if self.repository is None or self._tracker is None:
            return None
        try:
            order_time = datetime.fromtimestamp(order.timestamp, tz=UTC)
            symbol_pid = await resolve_symbol_public_id(
                self.repository, request.symbol, as_of=order_time
            )
            if symbol_pid is None:
                logger.error(f"No active Symbol row for {request.symbol}, cannot log order")
                return None
            _id, instrument_public_id = await self.repository.ensure_instrument(
                symbol_public_id=symbol_pid,
                exchange=self.exchange_name,
                session_id=self._tracker.session_id,
                sequence_id=self._tracker.next_sequence("instruments"),
                timestamp=order_time,
            )
            seq = self._tracker.next_sequence("orders")
            mode = (
                ExecutionModeEnum.PAPER.value
                if self.exchange_name == ExchangeEnum.PAPER.value
                else ExecutionModeEnum.LIVE.value
            )
            return await self.repository.insert_order(
                instrument_public_id=instrument_public_id,
                client_order_id=order.client_order_id,
                exchange_order_id=order.id,
                created_at=order_time,
                side=order.side.value,
                order_type=EXCHANGE_TO_CORE_ORDER_TYPE.get(request.type.value, request.type.value),
                price=order.price,
                size=order.amount,
                status=order.status.value,
                time_in_force=None,
                mode=mode,
                session_id=self._tracker.session_id,
                sequence_id=seq,
                timestamp=order_time,
                wallet_public_id=request.wallet_public_id,
                operator_public_id=request.operator_public_id,
                leverage=request.leverage,
                reduce_only=request.reduce_only,
            )
        except Exception as e:
            logger.error(f"Failed to log order to database: {e}")
            return None

    async def _log_order_update_to_db(
        self,
        db_order_id: int,
        status: ExchangeOrderStatusEnum,
        exchange_order_id: str | None = None,
        error: str | None = None,
        filled_size: float | None = None,
        average_price: float | None = None,
    ) -> int | None:
        """Close old order version and insert new one in the database (SCD Type 2).

        This internal method is called when an order status changes
        (e.g., filled, canceled, rejected). Returns the new version's
        integer id so callers can link executions to the latest row —
        the fill path MUST re-point ``PendingOrderState.db_order_id``
        to it, or the next version bump re-versions a closed row and
        trips the active-unique index.

        Args:
            db_order_id: Database order ID to close and version.
            status: New order status.
            exchange_order_id: Exchange order ID if it changed.
            error: Error message if order was rejected.
            filled_size: Venue-reported CUMULATIVE filled size — pass
                on fill updates so the order row stays truthful;
                ``None`` marks a status-only transition (fill columns
                carry forward).
            average_price: Venue-reported average fill price (raw —
                may be ``None`` even on fills; the repository then
                derives a VWAP from executions or stores NULL).

        Returns:
            New order version's integer id, or None if repository is
            not configured or operation fails.
        """
        if self.repository is None or self._tracker is None:
            return None
        try:
            now = datetime.now(tz=UTC)
            seq = self._tracker.next_sequence("orders")
            return await self.repository.update_order(
                order_id=db_order_id,
                status=status.value,
                updated_at=now,
                exchange_order_id=exchange_order_id,
                error=error,
                filled_size=filled_size,
                average_price=average_price,
                session_id=self._tracker.session_id,
                sequence_id=seq,
                timestamp=now,
            )
        except SQLAlchemyError as e:
            logger.error(f"Failed to log order update to database: {e}")
            return None

    async def _log_execution_to_db(
        self,
        order_public_id: str,
        execution: ExecutionUpdate,
        wallet_public_id: str,
        operator_public_id: str | None = None,
        delta_size: float | None = None,
        delta_price: float | None = None,
        fee: float | None = None,
        fee_asset: str | None = None,
        status: str | None = None,
    ) -> None:
        """Persist an execution (fill) to the database.

        When resolved values are provided (from _build_execution_data),
        they are used instead of re-deriving from raw execution fields.
        This ensures DB records match published ExecutionData.

        Args:
            order_public_id: Logical order identity (stable across versions).
            execution: Execution details (timestamp, side, exec_id, trade_id).
            wallet_public_id: Owning wallet for routing and
                NOT NULL schema compliance. Normally read by the caller
                from the per-wallet executor instance.
            operator_public_id: Trading identity that initiated the
                order this fill belongs to. Nullable because strategy-
                emitted orders have no human operator. Read by the
                caller from the pending order's
                ``OrderRequestData.operator_public_id``.
            delta_size: Resolved fill delta size. Falls back to raw execution fields.
            delta_price: Resolved fill price. Falls back to raw execution fields.
            fee: Resolved fee amount. Falls back to fee_usd_equiv.
            fee_asset: Resolved fee currency. Falls back to "USD".
            status: Resolved fill status. Falls back to to_fill_status().
        """
        if self.repository is None or self._tracker is None:
            return
        resolved_size = (
            delta_size
            if delta_size is not None
            else (execution.last_qty or execution.cum_qty or 0.0)
        )
        resolved_price = (
            delta_price
            if delta_price is not None
            else (execution.last_price or execution.average_price or 0.0)
        )
        resolved_fee = fee if fee is not None else (execution.fee_usd_equiv or 0.0)
        resolved_fee_asset = fee_asset if fee_asset is not None else "USD"
        resolved_status = status if status is not None else to_fill_status(execution)
        liq_map = {"m": "maker", "t": "taker"}
        resolved_liquidity = liq_map.get(getattr(execution, "liquidity_ind", None) or "", "unknown")
        try:
            seq = self._tracker.next_sequence("executions")
            await self.repository.insert_execution(
                order_public_id=order_public_id,
                timestamp=execution.timestamp,
                side=execution.side.value,
                status=resolved_status,
                price=resolved_price,
                size=resolved_size,
                fee=resolved_fee,
                fee_asset=resolved_fee_asset,
                wallet_public_id=wallet_public_id,
                operator_public_id=operator_public_id,
                exec_id=execution.exec_id,
                trade_id=str(execution.trade_id) if execution.trade_id is not None else None,
                session_id=self._tracker.session_id,
                sequence_id=seq,
                liquidity_role=resolved_liquidity,
            )
        except SQLAlchemyError as e:
            logger.error(f"Failed to log execution to database: {e}")
