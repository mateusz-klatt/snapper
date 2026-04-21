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
from snapper.data.repository_types import TradeCommandRow

PublishFn = Callable[[TradeCommandRow], Awaitable[None]]


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
        """
        self._repo = repository
        self._publish_fn = publish_fn
        self._poll_interval = poll_interval
        self._batch_size = batch_size
        self._ownership = ownership
        self._max_scan_rows = max_scan_rows
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

    async def _dispatch_batch(self) -> None:
        """Fetch and dispatch one batch of undispatched commands."""
        now = datetime.now(UTC)
        commands = await self._fetch_owned_batch(now)
        for cmd in commands:
            published = False
            try:
                if self._publish_fn is not None:
                    await self._publish_fn(cmd)
                published = True
                await self._repo.update_trade_command_status(
                    public_id=cmd["public_id"],
                    new_status=TradeCommandStatusEnum.DISPATCHED,
                    bus_time=datetime.now(UTC),
                    session_id=cmd["session_id"],
                    sequence_id=cmd["sequence_id"],
                    dispatched_at=datetime.now(UTC),
                    attempt_count=cmd["attempt_count"] + 1,
                )
                logger.debug(
                    f"OutboxDispatcher: dispatched command {cmd['public_id']} "
                    f"({cmd['exchange']}.{cmd['instrument']})"
                )
            except Exception:
                logger.exception(f"OutboxDispatcher: failed to dispatch command {cmd['public_id']}")
                if published:
                    logger.error(
                        f"OutboxDispatcher: command {cmd['public_id']} was published "
                        f"but DB update failed — NOT reverting to 'created' to prevent replay"
                    )
                else:
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

    def stop(self) -> None:
        """Signal the dispatcher to stop on next cycle."""
        self._running = False
        self._wake.set()
