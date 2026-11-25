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


@dataclass(frozen=True, slots=True)
class _CommandRetryState:
    """Per-command retry backoff and error-logging cadence state.

    Attributes:
        failures: Consecutive failure count for this command.
        next_at: Monotonic timestamp when the command is next eligible.
        first_failed_at: Monotonic timestamp of the initial failure in the streak.
        last_error_logged_at: Monotonic timestamp of the most recent ERROR log.
        last_seen: Monotonic timestamp when the command last appeared in a fetch page.
    """

    failures: int
    next_at: float
    first_failed_at: float
    last_error_logged_at: float
    last_seen: float


class OutboxDispatcher:
    """Polls TradeCommand table and publishes undispatched commands to ZMQ.

    Runs as an asyncio task inside the trade runtime process. Uses
    asyncio.Event for immediate wake-up on new commands, with 50ms
    polling as crash-recovery fallback.

    Under multi-instance partitioning
    every coordinator's OutboxDispatcher receives every ``created``
    row on a poll. The ``ownership`` kwarg filters rows in Python so
    each dispatcher only publishes its own shards. Scans paginate via
    the repository ``offset`` parameter up to ``max_scan_rows`` to
    avoid pathological starvation when foreign-owner rows dominate
    the backlog.

    Failing commands back off exponentially per ``public_id`` starting at
    0.25 s (factor 2), capped at 5 s for TTL-bound commands (``create``,
    ``submit``) and 60 s for non-TTL commands. Unresolved cancels remain
    quiet per-tick holds rather than failures. Per-shard FIFO is kept by
    deferring every later row on the same ``shard_key`` in fetch order
    whenever an earlier row on that shard is backing off, preventing a
    later cancel or submit from overtaking a pending command on that
    shard while letting unrelated shards keep flowing. Its cost is that a
    stuck non-TTL command blocks only its own shard, capped at 60 s per
    retry and re-logging an ERROR every 5 minutes.

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

    def _is_row_deferred(
        self,
        cmd: TradeCommandRow,
        deferred_shards: set[str],
        now_mono: float,
    ) -> bool:
        """Return whether a row is backing off or queued behind a deferred shard peer.

        Args:
            cmd: Candidate command row from the current fetch page.
            deferred_shards: Shard keys already deferred in fetch order this cycle.
            now_mono: Current monotonic clock reading in seconds.

        Returns:
            True when ``cmd`` must be deferred; False when eligible for dispatch.
        """
        shard_key = cmd["shard_key"]
        if shard_key in deferred_shards:
            return True
        entry = self._retry.get(cmd["public_id"])
        if entry is not None and entry.next_at > now_mono:
            deferred_shards.add(shard_key)
            return True
        return False

    def _filter_owned_rows(
        self,
        page: list[TradeCommandRow],
        owned: list[TradeCommandRow],
        deferred_shards: set[str],
        now_mono: float,
    ) -> None:
        """Append eligible, non-deferred rows from ``page`` to ``owned``.

        Stops once ``owned`` has reached ``batch_size``. When ``self._ownership``
        is set, foreign-shard rows are skipped; any row whose retry timer has
        not expired defers itself and every subsequent row on the same
        ``shard_key`` across pages in fetch order.

        Args:
            page: Current repository page of undispatched commands.
            owned: Accumulator list receiving eligible rows up to ``batch_size``.
            deferred_shards: Mutable set of shard keys deferred in fetch order.
            now_mono: Current monotonic clock reading in seconds.
        """
        ownership = self._ownership
        for cmd in page:
            if ownership is not None and not ownership.owns(cmd["shard_key"]):
                continue
            if self._is_row_deferred(cmd, deferred_shards, now_mono):
                continue
            owned.append(cmd)
            if len(owned) >= self._batch_size:
                return

    async def _fetch_owned_batch(self, now: datetime) -> list[TradeCommandRow]:
        """Fetch the next batch of eligible, non-deferred commands this instance owns.

        When ``self._ownership`` is ``None`` and no retry entry is currently
        deferred, this is a straight pass-through to
        :meth:`Repository.get_undispatched_commands` with ``limit=batch_size``.
        Otherwise, the method pages through the ``status='created'`` set with
        a larger page size, applies ownership (when configured) and per-shard
        FIFO deferral filtering in Python, and stops when it has collected
        ``batch_size`` rows OR the DB set is exhausted OR ``max_scan_rows`` is
        reached.

        Args:
            now: Temporal visibility cutoff passed as ``as_of``.

        Returns:
            Up to ``batch_size`` eligible, non-deferred rows owned by this
            coordinator.
        """
        now_mono = time.monotonic()
        has_deferred = any(entry.next_at > now_mono for entry in self._retry.values())
        if self._ownership is None and not has_deferred:
            page = await self._repo.get_undispatched_commands(as_of=now, limit=self._batch_size)
            self._touch_seen_rows(page, now_mono)
            self._prune_stale_retries(now_mono)
            return page
        owned: list[TradeCommandRow] = []
        deferred_shards: set[str] = set()
        offset = 0
        page_size = max(self._batch_size * 4, 20)
        max_scan = self._max_scan_rows
        while len(owned) < self._batch_size:
            page = await self._repo.get_undispatched_commands(
                as_of=now, limit=page_size, offset=offset
            )
            if not page:
                break
            self._touch_seen_rows(page, now_mono)
            self._filter_owned_rows(page, owned, deferred_shards, now_mono)
            offset += len(page)
            if max_scan is not None and offset >= max_scan and len(owned) < self._batch_size:
                logger.warning(
                    "OutboxDispatcher: scan reached max_scan_rows={} with only "
                    "{} owned rows found; partial batch. Consider raising the "
                    "cap or investigating shard skew.",
                    max_scan,
                    len(owned),
                )
                break
        self._prune_stale_retries(now_mono)
        return owned

    def _record_retry_failure(
        self,
        cmd: TradeCommandRow,
        *,
        error_message: str,
        reason: str,
        include_traceback: bool,
    ) -> None:
        """Schedule exponential per-command backoff and emit cadence-bounded logs.

        Logs ERROR on the initial failure and whenever 300 s of continuous
        failure have elapsed since the previous ERROR log; logs a single-line
        WARNING with ``public_id``, attempt count, and next delay in between.

        Args:
            cmd: Command whose dispatch, barrier, or TTL step failed.
            error_message: Message used when emitting an ERROR log.
            reason: Short single-line summary used in WARNING logs.
            include_traceback: Whether ERROR logs attach the active traceback.
        """
        now_mono = time.monotonic()
        public_id = cmd["public_id"]
        prev = self._retry.get(public_id)
        if prev is None:
            failures = 1
            first_failed_at = now_mono
            should_log_error = True
            last_error_logged_at = now_mono
        else:
            failures = prev.failures + 1
            first_failed_at = prev.first_failed_at
            should_log_error = (
                now_mono - prev.last_error_logged_at
            ) >= _RETRY_ERROR_RELOG_INTERVAL_S
            last_error_logged_at = now_mono if should_log_error else prev.last_error_logged_at
        cap = (
            _RETRY_BACKOFF_TTL_CAP_S
            if cmd["command_type"] in _EXPIRABLE_COMMAND_TYPES
            else _RETRY_BACKOFF_CAP_S
        )
        delay = min(_RETRY_BACKOFF_BASE_S * (_RETRY_BACKOFF_FACTOR ** min(failures - 1, 30)), cap)
        self._retry[public_id] = _CommandRetryState(
            failures=failures,
            next_at=now_mono + delay,
            first_failed_at=first_failed_at,
            last_error_logged_at=last_error_logged_at,
            last_seen=now_mono,
        )
        if should_log_error:
            if include_traceback:
                logger.exception(
                    "{} (attempt={}, next_delay={:.2f}s)",
                    error_message,
                    failures,
                    delay,
                )
            else:
                logger.error(
                    "{} (attempt={}, next_delay={:.2f}s)",
                    error_message,
                    failures,
                    delay,
                )
        else:
            logger.warning(
                "OutboxDispatcher: command {} {} (attempt={}, next_delay={:.2f}s)",
                public_id,
                reason,
                failures,
                delay,
            )

    async def _expire_if_stale(self, cmd: TradeCommandRow) -> bool:
        """Expire a stale create/submit command instead of dispatching it.

        Returns True when the command was handled here (expired, lost
        the expiry race, or deferred with backoff on probe/CAS error)
        and must NOT be published this cycle; False when it should
        dispatch normally.

        Decision table:

        - TTL disabled, non-expirable command type, or fresh → publish.
        - Stale WITH durable submit evidence → publish anyway with a
          WARN: the coordinator crashed after publishing but before the
          CREATED→DISPATCHED commit, so the order may be live — expiring
          would fabricate a terminal state for it (exactly the
          false-reject failure the UNKNOWN state exists to prevent).
          The executor's duplicate-submit guard absorbs the re-publish
          and the normal bulk path performs the missing transition.
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

        Returns:
            True when the row must be skipped by the publish loop.
        """
        ttl = self._dispatch_ttl_s
        if ttl is None or ttl <= 0:
            return False
        if cmd["command_type"] not in _EXPIRABLE_COMMAND_TYPES:
            return False
        now = datetime.now(UTC)
        age_s = (now - cmd["created_at"]).total_seconds()
        if age_s <= ttl:
            return False
        try:
            if await self._repo.has_order_submit_evidence(cmd["client_order_id"]):
                logger.warning(
                    f"OutboxDispatcher: command {cmd['public_id']} is stale "
                    f"(age {age_s:.1f}s > TTL {ttl:.1f}s) but durable submit evidence "
                    f"exists — publishing for the executor guard to dedup instead of "
                    f"expiring a possibly-live order"
                )
                return False
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
            return True
        self._retry.pop(cmd["public_id"], None)
        if not won:
            logger.info(
                f"OutboxDispatcher: lost the expiry race for {cmd['public_id']} "
                f"(status moved concurrently); skipping this cycle"
            )
            return True
        logger.warning(
            f"OutboxDispatcher: EXPIRED stale command {cmd['public_id']} "
            f"({cmd['client_order_id']}, age {age_s:.1f}s > TTL {ttl:.1f}s) — never published"
        )
        if self._expire_fn is not None:
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
        return True

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
        if await self._expire_if_stale(cmd):
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
                await self._repo.update_trade_command_status(
                    public_id=cmd["public_id"],
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

    async def _dispatch_batch(self) -> None:
        """Fetch one batch of undispatched commands, publish, and bulk-dispatch."""
        now = datetime.now(UTC)
        commands = await self._fetch_owned_batch(now)
        if not commands:
            return
        success_updates: list[TradeCommandDispatchUpdate] = []
        for cmd in commands:
            update = await self._dispatch_command(cmd)
            if update is not None:
                success_updates.append(update)
        if not success_updates:
            return
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

    def stop(self) -> None:
        """Signal the dispatcher to stop on next cycle."""
        self._running = False
        self._wake.set()
