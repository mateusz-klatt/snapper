"""Outbox dispatcher for durable trade command delivery.

Polls the TradeCommand table for undispatched commands and publishes
them to ZMQ. Primary dispatch path uses asyncio.Event for low-latency
wake-up after DB commit; polling at 50ms interval is the fallback for
crash recovery.
Standalone dispatcher coroutine, wired into trade runtime.
"""

import asyncio
import contextlib
import time
from collections.abc import Awaitable
from collections.abc import Callable
from dataclasses import dataclass
from dataclasses import replace
from datetime import UTC
from datetime import datetime
from enum import Enum
from enum import auto
from typing import Final

from loguru import logger

from snapper.application.trade.cancel_target import CancelTargetUnresolvedError
from snapper.core.command_material import TradeCommandMaterialChangedError
from snapper.core.partitioning import ShardOwnership
from snapper.core.types import TradeCommandStatusEnum
from snapper.data.repository import SQLAlchemyRepository
from snapper.data.repository_types import TradeCommandDispatchUpdate
from snapper.data.repository_types import TradeCommandRow

PublishFn = Callable[[TradeCommandRow], Awaitable[None]]
ExpireFn = Callable[[TradeCommandRow], Awaitable[None]]

_EXPIRABLE_COMMAND_TYPES: tuple[str, ...] = ("create", "submit")
"""Command types subject to the dispatch TTL.

Cancels (and replaces) are exempt: expiring a stale cancel strands a
live order, a late cancel carries no double-exposure risk, and its
client_order_id is the ORIGINAL order's — the submit-evidence probe
would false-positive on it.
"""

_RETRY_BACKOFF_BASE_S: Final[float] = 0.25
"""Initial per-command retry delay in seconds after a dispatch failure."""

_RETRY_BACKOFF_FACTOR: Final[int] = 2
"""Exponential multiplier applied to each consecutive command failure."""

_RETRY_BACKOFF_TTL_CAP_S: Final[float] = 5.0
"""Maximum retry delay in seconds for TTL-bound commands, well below the 30 s TTL."""

_RETRY_BACKOFF_CAP_S: Final[float] = 60.0
"""Maximum retry delay in seconds for non-TTL command types."""

_RETRY_ERROR_RELOG_INTERVAL_S: Final[float] = 300.0
"""Seconds of continuous failure between repeated ERROR logs for the same command."""

_RETRY_STALE_PRUNE_S: Final[float] = 300.0
"""Seconds since last fetch sighting after which a retry entry is pruned."""

_SHARD_SKIP_CAP_MULTIPLIER: Final[int] = 10
"""Multiplier on ``max_scan_rows`` capping non-stale shard-deferred skips."""


class _ExpiryOutcome(Enum):
    """Result of checking whether a command can be expired."""

    INELIGIBLE = auto()
    SUBMIT_EVIDENCE = auto()
    HANDLED = auto()


class _RetryKind(Enum):
    """Distinguish command backoff from an expiry probe cooldown."""

    BLOCKING_FAILURE = auto()
    NONBLOCKING_FAILURE = auto()
    EXPIRY_PROBE = auto()


@dataclass(frozen=True, slots=True)
class _CommandRetryState:
    """Per-command retry backoff and error-logging cadence state.

    Attributes:
        failures: Consecutive failure or evidence hold count for this command.
        next_at: Monotonic timestamp when the command or expiry probe is next eligible.
        first_failed_at: Monotonic timestamp of the initial failure in the streak.
        last_error_logged_at: Monotonic timestamp of the most recent ERROR log.
        last_seen: Monotonic timestamp when the command last appeared in a fetch page.
        kind: How this delay affects dispatch and other rows on the shard.
    """

    failures: int
    next_at: float
    first_failed_at: float
    last_error_logged_at: float
    last_seen: float
    kind: _RetryKind


@dataclass(frozen=True, slots=True)
class _FetchedBatch:
    """Owned commands split between normal dispatch and stale shard-deferred expiry.

    Attributes:
        dispatch: Eligible non-deferred commands to run through full dispatch.
        expire_only: Stale expirable rows deferred only by shard blocking.
    """

    dispatch: list[TradeCommandRow]
    expire_only: list[TradeCommandRow]


@dataclass(slots=True)
class _ScanState:
    """Mutable accumulator and scan-ceiling counters for one paginated poll.

    Attributes:
        dispatch: Eligible rows queued for dispatch up to ``batch_size``.
        expire_only: Stale shard-deferred expirable rows queued for expiry only.
        deferred_shards: Shard keys already deferred in fetch order this cycle.
        seen_public_ids: Public IDs already processed across keyset pages.
        now: Current UTC wall clock used to evaluate dispatch TTL staleness.
        now_mono: Current monotonic clock reading in seconds.
        considered: Rows counted against ``max_scan_rows``.
        shard_skipped: Non-stale shard-deferred rows counted against the skip ceiling.
        stop_reason: Cap reason when pagination terminates early, or ``None``.
    """

    dispatch: list[TradeCommandRow]
    expire_only: list[TradeCommandRow]
    deferred_shards: set[str]
    seen_public_ids: set[str]
    now: datetime
    now_mono: float
    considered: int = 0
    shard_skipped: int = 0
    stop_reason: str | None = None


def _keyset_cursor(cmd: TradeCommandRow) -> tuple[datetime, str, int]:
    """Build the ``(created_at, public_id, id)`` cursor tuple, failing loudly if ``id`` is missing.

    Args:
        cmd: Last command row returned by a repository keyset page.

    Returns:
        Strict ``(created_at, public_id, id)`` keyset cursor tuple.
    """
    return (cmd["created_at"], cmd["public_id"], cmd["id"])


class OutboxDispatcher:
    """Polls TradeCommand table and publishes undispatched commands to ZMQ.

    Runs as an asyncio task inside the trade runtime process. Uses
    asyncio.Event for immediate wake-up on new commands, with 50ms
    polling as crash-recovery fallback.

    Under multi-instance partitioning
    every coordinator's OutboxDispatcher receives every ``created``
    row on a poll. The ``ownership`` kwarg filters rows in Python so
    each dispatcher only publishes its own shards. Scans paginate via
    the repository keyset cursor ``after=(created_at, public_id, id)``
    rather than ``offset`` so concurrent status transitions between
    pages cannot shift rows and skip a backing-off shard blocker.

    Failing commands back off exponentially per ``public_id`` starting at
    0.25 s (factor 2), capped at 5 s for TTL-bound commands (``create``,
    ``submit``) and 60 s for non-TTL commands. Unresolved cancels remain
    quiet per-tick holds rather than failures. Per-shard FIFO defers
    later expirable rows (``create``, ``submit``) on the same ``shard_key``
    in fetch order whenever an earlier shard-blocking row on that shard is
    backing off, preventing a later submit from overtaking a pending
    command on that shard while letting unrelated shards keep flowing.
    Non-expirable rows (``cancel`` and others) are deferred only by their
    own retry entry: a failing head must never block a safety cancel of an
    independent live order (a cancel whose target is unpublished is already
    held cleanly by :class:`CancelTargetUnresolvedError`). Stale expirable
    rows deferred only behind a blocked shard (not by their own retry entry)
    move into an expire-only pass executed after committing any successful
    dispatches; that pass runs material consistency checks before
    :meth:`_expire_if_stale` without publishing, and if durable submit
    evidence prevents expiry it schedules a TTL-capped probe cooldown with
    INFO/WARNING cadence logging. The cooldown suppresses repeat expiry
    probes only while an earlier command blocks the shard. When eligible
    for dispatch, the command checks material and TTL through the normal
    path, including a fresh evidence probe if stale. Expiry is terminal and
    publishes nothing, so shard order does not matter for it, whereas rows
    deferred by their own failure backoff are re-checked when due.

    Pagination bounds scan work with two caps when ``max_scan_rows`` is
    set: ``max_scan_rows`` counts considered rows (dispatch candidates,
    expire-only rows, own-retry skips, and foreign-shard skips), while
    non-stale rows skipped only because their shard is blocked count toward
    a separate ``10 * max_scan_rows`` ceiling so a backed-off non-TTL head
    cannot starve healthy shards (both remain unbounded when
    ``max_scan_rows`` is ``None``). Known limit: with the dispatch TTL
    disabled, a stuck shard head plus more than ``10 * max_scan_rows``
    blocked followers can still hide later rows on other shards, because the
    keyset cursor restarts on every poll; with TTL enabled, stale followers
    drain via expiry. A create held by the durable material barrier backs off
    without blocking its shard: the barrier runs before TTL expiry, so such
    a hold can persist indefinitely, and later commands on the shard never
    depend on a create that cannot be published.

    Args:
        repository: Database repository for reading/updating commands.
        publish_fn: Async callback that publishes a command dict to ZMQ.
            Signature: async (command_row) -> None.
        poll_interval: Seconds between fallback polls. Defaults to 0.05.
        batch_size: Max commands per poll cycle. Defaults to 10.
        ownership: shard-ownership filter. ``None`` skips
            filtering.
        max_scan_rows: Operator-tunable cap on rows scanned per poll
            when filtering by ownership or skipping deferred rows.
            ``None`` = unbounded. Default in production is wired from
            ``AppSettings.coordinator_outbox_max_scan_rows`` (1000).
        dispatch_ttl_s: Max age in seconds before TTL-bound commands expire.
        expire_fn: Async callback invoked after a command expires via CAS.
    """

    def __init__(
        self,
        repository: SQLAlchemyRepository,
        publish_fn: PublishFn | None = None,
        poll_interval: float = 0.05,
        batch_size: int = 10,
        *,
        ownership: ShardOwnership | None = None,
        max_scan_rows: int | None = None,
        dispatch_ttl_s: float | None = None,
        expire_fn: ExpireFn | None = None,
    ) -> None:
        """Initialize outbox dispatcher.

        Args:
            repository: Database repository for TradeCommand access.
            publish_fn: Async callback to publish command to ZMQ.
            poll_interval: Fallback polling interval in seconds.
            batch_size: Maximum commands per poll cycle.
            ownership: shard-ownership filter (opt-in).
            max_scan_rows: Pagination safety cap for ownership-filter
                scans. ``None`` = scan until DB exhaustion.
            dispatch_ttl_s: Max age (seconds, from ``created_at``) a
                create/submit command may reach before dispatch expires
                it instead of publishing. ``None`` (and any
                value ``<= 0``) disables the gate.
            expire_fn: Async callback invoked after a command is
                CAS-transitioned to EXPIRED, so the coordinator can
                release the engine's in-flight intent and project the
                terminal state.
        """
        self._repo = repository
        self._publish_fn = publish_fn
        self._poll_interval = poll_interval
        self._batch_size = batch_size
        self._ownership = ownership
        self._max_scan_rows = max_scan_rows
        self._dispatch_ttl_s = dispatch_ttl_s
        self._expire_fn = expire_fn
        self._wake = asyncio.Event()
        self._running = False
        self._retry: dict[str, _CommandRetryState] = {}
        self._tick_blocked_shards: set[str] = set()

    def notify(self) -> None:
        """Signal the dispatcher that a new command was written.

        Called by TradingEngineService after committing a TradeCommand
        to DB. Wakes the dispatcher immediately instead of waiting for
        the next poll cycle.
        """
        self._wake.set()

    async def run(self) -> None:
        """Run the outbox dispatch loop.

        Polls for undispatched commands, publishes each via publish_fn,
        and updates command status to 'dispatched'. Runs until cancelled.
        """
        self._running = True
        logger.info("OutboxDispatcher started")
        try:
            while True:
                with contextlib.suppress(TimeoutError):
                    await asyncio.wait_for(self._wake.wait(), timeout=self._poll_interval)
                self._wake.clear()

                if not self._running:
                    break

                await self._dispatch_batch()
        except asyncio.CancelledError:
            logger.info("OutboxDispatcher cancelled")
            raise
        finally:
            self._running = False

    def _touch_seen_rows(self, page: list[TradeCommandRow], now_mono: float) -> None:
        """Refresh ``last_seen`` for retry entries appearing in a fetched page.

        Args:
            page: Page of undispatched command rows returned by the repository.
            now_mono: Current monotonic clock reading in seconds.
        """
        if not self._retry:
            return
        for cmd in page:
            public_id = cmd["public_id"]
            existing = self._retry.get(public_id)
            if existing is not None:
                self._retry[public_id] = replace(existing, last_seen=now_mono)

    def _prune_stale_retries(self, now_mono: float) -> None:
        """Drop retry entries not seen in fetched pages within the prune window.

        Args:
            now_mono: Current monotonic clock reading in seconds.
        """
        stale_ids = [
            public_id
            for public_id, entry in self._retry.items()
            if (now_mono - entry.last_seen) > _RETRY_STALE_PRUNE_S
        ]
        for public_id in stale_ids:
            del self._retry[public_id]

    def _is_stale_expirable(self, cmd: TradeCommandRow, now: datetime) -> bool:
        """Return whether an expirable row exceeds the enabled dispatch TTL.

        Args:
            cmd: Expirable candidate command row.
            now: Current UTC timestamp for age evaluation.

        Returns:
            True when ``dispatch_ttl_s`` is positive and ``cmd`` is older than it.
        """
        ttl = self._dispatch_ttl_s
        if ttl is None or ttl <= 0:
            return False
        return (now - cmd["created_at"]).total_seconds() > ttl

    def _classify_owned_row(self, cmd: TradeCommandRow, state: _ScanState) -> bool:
        """Route one owned row into dispatch, expire-only, or deferred skip.

        Rows deferred by their own failure backoff stay skipped (including
        stale ones, which re-check TTL when due) and count toward
        ``max_scan_rows``. An evidence probe cooldown defers the row only
        while it is already behind a shard blocker.
        Expirable rows deferred only behind a shard-blocking peer move into
        ``expire_only`` when stale (counting toward ``max_scan_rows``) or
        increment ``shard_skipped`` when non-stale.

        Args:
            cmd: Owned command row from the current keyset page.
            state: Mutable scan accumulator and counters.

        Returns:
            True when ``cmd`` was appended to ``state.dispatch``; False otherwise.
        """
        shard_key = cmd["shard_key"]
        behind_blocker = (
            cmd["command_type"] in _EXPIRABLE_COMMAND_TYPES and shard_key in state.deferred_shards
        )
        entry = self._retry.get(cmd["public_id"])
        if (
            entry is not None
            and entry.next_at > state.now_mono
            and (entry.kind is not _RetryKind.EXPIRY_PROBE or behind_blocker)
        ):
            if entry.kind is _RetryKind.BLOCKING_FAILURE:
                state.deferred_shards.add(shard_key)
            state.considered += 1
            return False
        if behind_blocker:
            if self._is_stale_expirable(cmd, state.now):
                state.expire_only.append(cmd)
                state.considered += 1
            else:
                state.shard_skipped += 1
            return False
        state.dispatch.append(cmd)
        state.considered += 1
        return True

    def _filter_owned_rows(self, page: list[TradeCommandRow], state: _ScanState) -> None:
        """Filter one keyset page into dispatch and expire-only lists within caps.

        Deduplicates by ``public_id`` across pages (keeping the first
        occurrence). Stops immediately once ``dispatch`` reaches
        ``batch_size``, ``considered`` reaches ``max_scan_rows``, or
        ``shard_skipped`` reaches ``10 * max_scan_rows``.

        Args:
            page: Current repository page of undispatched commands.
            state: Mutable scan accumulator and ceiling counters.
        """
        max_scan = self._max_scan_rows
        shard_skip_limit = None if max_scan is None else max_scan * _SHARD_SKIP_CAP_MULTIPLIER
        for cmd in page:
            public_id = cmd["public_id"]
            if public_id in state.seen_public_ids:
                continue
            state.seen_public_ids.add(public_id)
            if self._ownership is not None and not self._ownership.owns(cmd["shard_key"]):
                state.considered += 1
            elif self._classify_owned_row(cmd, state):
                if len(state.dispatch) >= self._batch_size:
                    return
            if max_scan is not None and state.considered >= max_scan:
                state.stop_reason = "scan"
                return
            if shard_skip_limit is not None and state.shard_skipped >= shard_skip_limit:
                state.stop_reason = "shard_skip"
                return

    def _log_scan_cap_warning(self, state: _ScanState) -> None:
        """Emit a warning when pagination halts on a configured scan ceiling.

        Args:
            state: Completed scan state carrying ``stop_reason`` and counts.
        """
        max_scan = self._max_scan_rows
        if state.stop_reason == "shard_skip" and max_scan is not None:
            ceiling = max_scan * _SHARD_SKIP_CAP_MULTIPLIER
            logger.warning(
                "OutboxDispatcher: scan reached shard-skip ceiling {} "
                "(10 * max_scan_rows={}) with only {} owned rows found; partial batch.",
                ceiling,
                max_scan,
                len(state.dispatch),
            )
            return
        logger.warning(
            "OutboxDispatcher: scan reached max_scan_rows={} with only "
            "{} owned rows found; partial batch. Consider raising the "
            "cap or investigating shard skew.",
            max_scan,
            len(state.dispatch),
        )

    async def _fetch_owned_batch(self, now: datetime) -> _FetchedBatch:
        """Fetch the next dispatch and expire-only batches owned by this instance.

        When ``self._ownership`` is ``None`` and no retry entry is currently
        deferred, this is a straight pass-through to
        :meth:`Repository.get_undispatched_commands` with ``limit=batch_size``.
        Otherwise, the method pages by keyset ``after=(created_at, public_id,
        id)`` (never ``offset``), requires ``id`` on the cursor row (failing
        loudly with :class:`KeyError` if omitted), deduplicates repeated
        ``public_id`` rows keeping the first occurrence, applies ownership and
        per-shard FIFO deferral filtering, routes stale shard-blocked expirable
        rows to ``expire_only``, and stops when ``dispatch`` collects
        ``batch_size`` rows, the DB set is exhausted, considered rows reach
        ``max_scan_rows``, or non-stale shard-blocked skips reach
        ``10 * max_scan_rows`` (both caps remain unbounded when
        ``max_scan_rows`` is ``None``). Known limit: with dispatch TTL
        disabled, a stuck shard head plus more than ``10 * max_scan_rows``
        blocked followers hides later shards because each poll restarts from
        the beginning of the queue; with TTL enabled, stale followers drain
        by expiry.

        Args:
            now: Temporal visibility cutoff passed as ``as_of``.

        Returns:
            :class:`_FetchedBatch` holding up to ``batch_size`` dispatchable
            rows plus any stale shard-deferred rows to expire.
        """
        now_mono = time.monotonic()
        has_deferred = any(entry.next_at > now_mono for entry in self._retry.values())
        if self._ownership is None and not has_deferred:
            page = await self._repo.get_undispatched_commands(as_of=now, limit=self._batch_size)
            self._touch_seen_rows(page, now_mono)
            self._prune_stale_retries(now_mono)
            return _FetchedBatch(dispatch=page, expire_only=[])
        state = _ScanState(
            dispatch=[],
            expire_only=[],
            deferred_shards=set(),
            seen_public_ids=set(),
            now=now,
            now_mono=now_mono,
        )
        after: tuple[datetime, str, int] | None = None
        page_size = max(self._batch_size * 4, 20)
        while len(state.dispatch) < self._batch_size:
            page = await self._repo.get_undispatched_commands(
                as_of=now, limit=page_size, after=after
            )
            if not page:
                break
            self._touch_seen_rows(page, now_mono)
            self._filter_owned_rows(page, state)
            if state.stop_reason is not None:
                self._log_scan_cap_warning(state)
                break
            after = _keyset_cursor(page[-1])
        self._prune_stale_retries(now_mono)
        return _FetchedBatch(dispatch=state.dispatch, expire_only=state.expire_only)

    def _schedule_retry_and_log(
        self,
        cmd: TradeCommandRow,
        *,
        message: str,
        reason: str,
        kind: _RetryKind,
        level: str = "exception",
    ) -> None:
        """Schedule exponential per-command backoff and emit cadence-bounded logs.

        Logs at ``level`` (``"exception"``, ``"error"``, or ``"info"``) on the
        initial entry and whenever 300 s elapse since the prior primary log;
        logs a single-line WARNING with ``public_id``, attempt count, and next
        delay in between.

        Args:
            cmd: Command entering or extending per-command backoff.
            message: Primary message emitted when the 300 s cadence fires.
            reason: Short single-line summary used in intermediate WARNING logs.
            kind: Whether this delay gates dispatch or only repeat expiry probes.
            level: Loguru level name for the primary 300 s cadence log.
        """
        now_mono = time.monotonic()
        public_id = cmd["public_id"]
        prev = self._retry.get(public_id)
        is_probe = kind is _RetryKind.EXPIRY_PROBE
        if prev is None or (prev.kind is _RetryKind.EXPIRY_PROBE) != is_probe:
            failures = 1
            first_failed_at = now_mono
            should_log_primary = True
            last_error_logged_at = now_mono
        else:
            failures = prev.failures + 1
            first_failed_at = prev.first_failed_at
            should_log_primary = (
                now_mono - prev.last_error_logged_at
            ) >= _RETRY_ERROR_RELOG_INTERVAL_S
            last_error_logged_at = now_mono if should_log_primary else prev.last_error_logged_at
        cap = (
            _RETRY_BACKOFF_TTL_CAP_S
            if cmd["command_type"] in _EXPIRABLE_COMMAND_TYPES
            else _RETRY_BACKOFF_CAP_S
        )
        delay = min(_RETRY_BACKOFF_BASE_S * (_RETRY_BACKOFF_FACTOR ** min(failures - 1, 30)), cap)
        if kind is _RetryKind.BLOCKING_FAILURE:
            self._tick_blocked_shards.add(cmd["shard_key"])
        self._retry[public_id] = _CommandRetryState(
            failures=failures,
            next_at=now_mono + delay,
            first_failed_at=first_failed_at,
            last_error_logged_at=last_error_logged_at,
            last_seen=now_mono,
            kind=kind,
        )
        if not should_log_primary:
            logger.warning(
                "OutboxDispatcher: command {} {} (attempt={}, next_delay={:.2f}s)",
                public_id,
                reason,
                failures,
                delay,
            )
            return
        if level == "exception":
            logger.exception("{} (attempt={}, next_delay={:.2f}s)", message, failures, delay)
        elif level == "info":
            logger.info("{} (attempt={}, next_delay={:.2f}s)", message, failures, delay)
        else:
            logger.error("{} (attempt={}, next_delay={:.2f}s)", message, failures, delay)

    def _record_retry_failure(
        self,
        cmd: TradeCommandRow,
        *,
        error_message: str,
        reason: str,
        include_traceback: bool,
        blocks_shard: bool = True,
    ) -> None:
        """Schedule exponential per-command backoff and emit ERROR/WARNING cadence logs.

        Args:
            cmd: Command whose dispatch, barrier, or TTL step failed.
            error_message: Message used when emitting an ERROR log.
            reason: Short single-line summary used in WARNING logs.
            include_traceback: Whether ERROR logs attach the active traceback.
            blocks_shard: Whether later rows on the same shard wait behind this command.
        """
        level = "exception" if include_traceback else "error"
        self._schedule_retry_and_log(
            cmd,
            message=error_message,
            reason=reason,
            kind=_RetryKind.BLOCKING_FAILURE if blocks_shard else _RetryKind.NONBLOCKING_FAILURE,
            level=level,
        )

    async def _invoke_expire_callback(self, cmd: TradeCommandRow) -> None:
        """Invoke the configured expiry release callback, logging CRITICAL on failure.

        Args:
            cmd: Command that transitioned to EXPIRED via CAS.
        """
        if self._expire_fn is None:
            return
        try:
            await self._expire_fn(cmd)
        except Exception:
            logger.critical(
                f"OutboxDispatcher: expiry release callback failed for "
                f"{cmd['public_id']} AFTER the EXPIRED transition — engine intent "
                f"self-heals via its in-flight timeout valve and the paired-group "
                f"deadlines cover the leg, but verify shard "
                f"{cmd['shard_key']} manually"
            )

    async def _expire_if_stale(
        self,
        cmd: TradeCommandRow,
        *,
        publish_on_evidence: bool = True,
    ) -> _ExpiryOutcome:
        """Expire a stale create/submit command instead of dispatching it.

        Decision table:

        - TTL disabled, non-expirable command type, or fresh → do not expire.
        - Stale WITH durable submit evidence → do not expire: when called on
          the dispatch path (``publish_on_evidence=True``), publish anyway
          with a WARN so the executor's duplicate-submit guard absorbs the
          replay; when called on the expire-only path, callers schedule a
          probe cooldown instead of publishing behind a shard head.
        - Stale without evidence → CAS CREATED→EXPIRED (race-safe
          against concurrent dispatchers and the guard scanner's
          CREATED→CANCELLED), clearing any retry state; only the CAS
          winner invokes ``expire_fn`` so the engine releases its
          in-flight intent exactly once.
        - Probe/CAS failure → schedule per-command backoff and defer
          (logged with ERROR/WARNING cadence); publishing a known-stale
          order on a DB blip would defeat the gate.

        Args:
            cmd: Candidate command row from the fetched batch.
            publish_on_evidence: Whether the caller will publish on submit
                evidence, which is logged here.

        Returns:
            INELIGIBLE if TTL does not apply or the command is still fresh,
            SUBMIT_EVIDENCE if durable evidence prevents expiry, and HANDLED
            if the command expired, lost the expiry race, or backed off on error.
        """
        ttl = self._dispatch_ttl_s
        if ttl is None or ttl <= 0:
            return _ExpiryOutcome.INELIGIBLE
        if cmd["command_type"] not in _EXPIRABLE_COMMAND_TYPES:
            return _ExpiryOutcome.INELIGIBLE
        now = datetime.now(UTC)
        age_s = (now - cmd["created_at"]).total_seconds()
        if age_s <= ttl:
            return _ExpiryOutcome.INELIGIBLE
        try:
            if await self._repo.has_order_submit_evidence(cmd["client_order_id"]):
                if publish_on_evidence:
                    logger.warning(
                        f"OutboxDispatcher: command {cmd['public_id']} is stale "
                        f"(age {age_s:.1f}s > TTL {ttl:.1f}s) but durable submit evidence "
                        f"exists — publishing for the executor guard to dedup instead of "
                        f"expiring a possibly-live order"
                    )
                return _ExpiryOutcome.SUBMIT_EVIDENCE
            won = await self._repo.cas_trade_command_status(
                public_id=cmd["public_id"],
                expected_status=TradeCommandStatusEnum.CREATED,
                new_status=TradeCommandStatusEnum.EXPIRED,
                bus_time=now,
                session_id=cmd["session_id"],
                sequence_id=cmd["sequence_id"],
                terminal_at=now,
                last_error=f"expired by outbox dispatch TTL (age {age_s:.1f}s > {ttl:.1f}s)",
            )
        except Exception:
            self._record_retry_failure(
                cmd,
                error_message=(
                    f"OutboxDispatcher: TTL handling failed for {cmd['public_id']} — "
                    "deferring the stale row rather than publishing it"
                ),
                reason="TTL handling failed",
                include_traceback=True,
            )
            return _ExpiryOutcome.HANDLED
        self._retry.pop(cmd["public_id"], None)
        if not won:
            logger.info(
                f"OutboxDispatcher: lost the expiry race for {cmd['public_id']} "
                f"(status moved concurrently); skipping this cycle"
            )
            return _ExpiryOutcome.HANDLED
        logger.warning(
            f"OutboxDispatcher: EXPIRED stale command {cmd['public_id']} "
            f"({cmd['client_order_id']}, age {age_s:.1f}s > TTL {ttl:.1f}s) — never published"
        )
        await self._invoke_expire_callback(cmd)
        return _ExpiryOutcome.HANDLED

    async def _material_permits_dispatch(self, command: TradeCommandRow) -> bool:
        """Hold a durably blocked logical CREATE with backoff while retaining healthy peers.

        Proof/storage failures propagate out of the dispatcher instead of
        creating retry versions or backing off. Cancels remain available;
        all execution CREATE intents require proof.

        Args:
            command: Candidate command row to verify before expiry/publish.

        Returns:
            True when dispatch may proceed; False when held by a material barrier.
        """
        if command["command_type"] not in _EXPIRABLE_COMMAND_TYPES:
            return True
        try:
            await self._repo.require_consistent_trade_command_material(command["public_id"])
        except TradeCommandMaterialChangedError as exc:
            self._record_retry_failure(
                command,
                error_message=(
                    f"OutboxDispatcher: CREATE held by durable material barrier "
                    f"({command['public_id']}): {exc}"
                ),
                reason="held by durable material barrier",
                include_traceback=False,
                blocks_shard=False,
            )
            return False
        return True

    async def _publish_or_hold(self, cmd: TradeCommandRow) -> bool:
        """Publish when possible and hold only unresolved cancels without mutation.

        Args:
            cmd: Durable command selected for dispatch.

        Returns:
            False for an unresolved cancel; True after publish or when publishing is disabled.
        """
        try:
            if self._publish_fn is not None:
                await self._publish_fn(cmd)
        except CancelTargetUnresolvedError:
            return False
        return True

    async def _dispatch_command(self, cmd: TradeCommandRow) -> TradeCommandDispatchUpdate | None:
        """Publish or hold one command and return its dispatch update on success.

        Keeps check order: material consistency, then TTL expiry, then publish.
        Removes any tracked retry entry when a dispatch update is produced.

        Args:
            cmd: Candidate command from the current undispatched batch.

        Returns:
            CREATED-to-DISPATCHED update, or None when held or failed.
        """
        if not await self._material_permits_dispatch(cmd):
            return None
        if await self._expire_if_stale(cmd) is _ExpiryOutcome.HANDLED:
            return None
        try:
            if not await self._publish_or_hold(cmd):
                return None
        except Exception:
            self._record_retry_failure(
                cmd,
                error_message=f"OutboxDispatcher: failed to dispatch command {cmd['public_id']}",
                reason="dispatch failed",
                include_traceback=True,
            )
            try:
                won = await self._repo.cas_trade_command_status(
                    public_id=cmd["public_id"],
                    expected_status=TradeCommandStatusEnum.CREATED,
                    new_status=TradeCommandStatusEnum.CREATED,
                    bus_time=datetime.now(UTC),
                    session_id=cmd["session_id"],
                    sequence_id=cmd["sequence_id"],
                    attempt_count=cmd["attempt_count"] + 1,
                    last_error="dispatch failed",
                )
            except Exception:
                logger.exception(
                    f"OutboxDispatcher: failed to update command {cmd['public_id']} status"
                )
            else:
                if not won:
                    self._retry.pop(cmd["public_id"], None)
                    logger.info(
                        f"OutboxDispatcher: command {cmd['public_id']} moved concurrently "
                        "before failure update; cleared retry state"
                    )
            return None
        self._retry.pop(cmd["public_id"], None)
        bus_time = datetime.now(UTC)
        return TradeCommandDispatchUpdate(
            public_id=cmd["public_id"],
            bus_time=bus_time,
            session_id=cmd["session_id"],
            sequence_id=cmd["sequence_id"],
            dispatched_at=bus_time,
            attempt_count=cmd["attempt_count"] + 1,
        )

    async def _handle_expire_only(self, cmd: TradeCommandRow) -> None:
        """Verify material consistency then expire a stale shard-deferred row without publishing.

        Args:
            cmd: Stale expirable row deferred solely behind a shard blocker.
        """
        if not await self._material_permits_dispatch(cmd):
            return
        outcome = await self._expire_if_stale(cmd, publish_on_evidence=False)
        if outcome is not _ExpiryOutcome.SUBMIT_EVIDENCE:
            return
        self._schedule_retry_and_log(
            cmd,
            message=(
                f"OutboxDispatcher: command {cmd['public_id']} "
                "stale with submit evidence; waiting behind shard head"
            ),
            reason="stale with submit evidence; waiting behind shard head",
            kind=_RetryKind.EXPIRY_PROBE,
            level="info",
        )

    async def _dispatch_batch(self) -> None:
        """Fetch one batch of undispatched commands, publish, commit, and expire stale peers.

        Per-shard FIFO for expirable commands (``create``, ``submit``) also
        holds inside the batch: once a command fails in a way that blocks its
        shard, later expirable commands on that shard are skipped for the rest
        of this tick and deferred on subsequent fetches behind the failing
        command. Non-expirable commands (``cancel`` and others) are never
        skipped behind a shard-blocking peer. Successful dispatches commit
        first so the replay window stays narrow, and only then do stale
        shard-deferred expirable rows run material consistency checks followed
        by :meth:`_expire_if_stale` (never publishing; durable submit evidence
        records a cooldown for repeat expiry probes while the shard is blocked).
        Material-barrier holds and unresolved-cancel holds do not block the
        shard.
        """
        now = datetime.now(UTC)
        batch = await self._fetch_owned_batch(now)
        if not batch.dispatch and not batch.expire_only:
            return
        self._tick_blocked_shards.clear()
        success_updates: list[TradeCommandDispatchUpdate] = []
        for cmd in batch.dispatch:
            if (
                cmd["command_type"] in _EXPIRABLE_COMMAND_TYPES
                and cmd["shard_key"] in self._tick_blocked_shards
            ):
                continue
            update = await self._dispatch_command(cmd)
            if update is not None:
                success_updates.append(update)
        if success_updates:
            try:
                await self._repo.bulk_dispatch_trade_commands(success_updates)
                for update in success_updates:
                    logger.debug(f"OutboxDispatcher: dispatched command {update['public_id']}")
            except Exception:
                logger.exception(
                    "OutboxDispatcher: bulk dispatch DB write failed for "
                    f"{len(success_updates)} published commands — rows remain 'created' "
                    "and WILL re-publish on the next tick; the executor-side "
                    "duplicate-submit guard is the dedup boundary"
                )
        for cmd in batch.expire_only:
            await self._handle_expire_only(cmd)

    def stop(self) -> None:
        """Signal the dispatcher to stop on next cycle."""
        self._running = False
        self._wake.set()
