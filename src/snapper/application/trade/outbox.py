"""Outbox dispatcher for durable trade command delivery.

Polls the TradeCommand table for undispatched commands and publishes
them to ZMQ. Primary dispatch path uses asyncio.Event for low-latency
wake-up after DB commit; polling at 50ms interval is the fallback for
crash recovery.
Standalone dispatcher coroutine, wired into trade runtime.
"""

import asyncio
import contextlib
from collections.abc import Awaitable
from collections.abc import Callable
from datetime import UTC
from datetime import datetime

from loguru import logger

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

    Args:
        repository: Database repository for reading/updating commands.
        publish_fn: Async callback that publishes a command dict to ZMQ.
            Signature: async (command_row) -> None.
        poll_interval: Seconds between fallback polls. Defaults to 0.05.
        batch_size: Max commands per poll cycle. Defaults to 10.
        ownership: shard-ownership filter. ``None`` skips
            filtering.
        max_scan_rows: Operator-tunable cap on rows scanned per poll
            when filtering by ownership. ``None`` = unbounded.
            Default in production is wired from
            ``AppSettings.coordinator_outbox_max_scan_rows`` (1000).
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

    def _filter_owned_rows(
        self,
        page: list[TradeCommandRow],
        ownership: ShardOwnership,
        owned: list[TradeCommandRow],
    ) -> None:
        """Append rows from ``page`` owned by ``ownership`` to ``owned``.

        Stops once ``owned`` has reached ``batch_size``. Extracted from
        :meth:`_fetch_owned_batch` to keep the paging loop's cognitive
        complexity under the repo threshold.
        """
        for cmd in page:
            if ownership.owns(cmd["shard_key"]):
                owned.append(cmd)
                if len(owned) >= self._batch_size:
                    return

    async def _fetch_owned_batch(self, now: datetime) -> list[TradeCommandRow]:
        """Fetch the next batch of commands this coordinator owns.

        When ``self._ownership`` is ``None``
        this is a straight pass-through to
        meth:`Repository.get_undispatched_commands` with ``limit=batch_size``.
        When ``self._ownership`` is set, the method pages through the
        ``status='created'`` set with a larger page size, filters in
        Python, and stops when it has collected ``batch_size`` owned
        rows OR the DB set is exhausted OR ``max_scan_rows`` is
        reached. The cap prevents pathological starvation when a
        foreign owner's backlog dominates the head of the queue; a
        WARN log surfaces the hit so operators can raise the cap or
        investigate skew.

        Returns:
            Up to ``batch_size`` rows owned by this coordinator.
        """
        if self._ownership is None:
            return await self._repo.get_undispatched_commands(as_of=now, limit=self._batch_size)
        owned: list[TradeCommandRow] = []
        offset = 0
        page_size = max(self._batch_size * 4, 20)
        max_scan = self._max_scan_rows
        while len(owned) < self._batch_size:
            page = await self._repo.get_undispatched_commands(
                as_of=now, limit=page_size, offset=offset
            )
            if not page:
                break
            self._filter_owned_rows(page, self._ownership, owned)
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
        return owned

    async def _expire_if_stale(self, cmd: TradeCommandRow) -> bool:
        """Expire a stale create/submit command instead of dispatching it.

        Returns True when the command was handled here (expired, lost
        the expiry race, or left for retry on an error) and must NOT be
        published this cycle; False when it should dispatch normally.

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
          CREATED→CANCELLED); only the CAS winner invokes ``expire_fn``
          so the engine releases its in-flight intent exactly once.
        - Probe/CAS failure → defer the row to the next 50ms tick
          (logged); publishing a known-stale order on a DB blip would
          defeat the gate.

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
            logger.exception(
                f"OutboxDispatcher: TTL handling failed for {cmd['public_id']} — "
                f"deferring the stale row to the next tick rather than publishing it"
            )
            return True
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

    async def _dispatch_batch(self) -> None:
        """Fetch and dispatch one batch of undispatched commands.

        Publishes each command sequentially (ZMQ broker ordering is
        preserved), then commits the success-set's CREATED -> DISPATCHED
        SCD2 transitions in a single bulk call. Per-row revert path
        (publish failed) still uses ``update_trade_command_status`` —
        bulk-write blow-up only risks the success path, not the
        error-recovery path.
        """
        now = datetime.now(UTC)
        commands = await self._fetch_owned_batch(now)
        success_updates: list[TradeCommandDispatchUpdate] = []
        published_pids: list[str] = []
        for cmd in commands:
            if await self._expire_if_stale(cmd):
                continue
            try:
                if self._publish_fn is not None:
                    await self._publish_fn(cmd)
                bus_time = datetime.now(UTC)
                success_updates.append(
                    TradeCommandDispatchUpdate(
                        public_id=cmd["public_id"],
                        bus_time=bus_time,
                        session_id=cmd["session_id"],
                        sequence_id=cmd["sequence_id"],
                        dispatched_at=bus_time,
                        attempt_count=cmd["attempt_count"] + 1,
                    )
                )
                published_pids.append(cmd["public_id"])
            except Exception:
                logger.exception(f"OutboxDispatcher: failed to dispatch command {cmd['public_id']}")
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
        if not success_updates:
            return
        try:
            await self._repo.bulk_dispatch_trade_commands(success_updates)
            for pid in published_pids:
                logger.debug(f"OutboxDispatcher: dispatched command {pid}")
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
