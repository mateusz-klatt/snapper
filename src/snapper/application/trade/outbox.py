"""Outbox dispatcher for durable trade command delivery.

Polls the TradeCommand table for undispatched commands and publishes
them to ZMQ. Primary dispatch path uses asyncio.Event for low-latency
wake-up after DB commit; polling at 50ms interval is the fallback for
crash recovery.

Phase 1c: standalone dispatcher coroutine, wired into trade runtime.
"""

import asyncio
import contextlib
from collections.abc import Awaitable
from collections.abc import Callable
from datetime import UTC
from datetime import datetime

from loguru import logger

from snapper.application.trade.divergence_detector import DivergenceDetector
from snapper.core.types import TradeCommandStatusEnum
from snapper.data.repository import SQLAlchemyRepository
from snapper.data.repository_types import TradeCommandRow

PublishFn = Callable[[TradeCommandRow], Awaitable[None]]


class OutboxDispatcher:
    """Polls TradeCommand table and publishes undispatched commands to ZMQ.

    Runs as an asyncio task inside the trade runtime process. Uses
    asyncio.Event for immediate wake-up on new commands, with 50ms
    polling as crash-recovery fallback.

    Args:
        repository: Database repository for reading/updating commands.
        publish_fn: Async callback that publishes a command dict to ZMQ.
            Signature: async (command_row) -> None.
        poll_interval: Seconds between fallback polls. Defaults to 0.05.
        batch_size: Max commands per poll cycle. Defaults to 10.
    """

    def __init__(
        self,
        repository: SQLAlchemyRepository,
        publish_fn: PublishFn | None = None,
        poll_interval: float = 0.05,
        batch_size: int = 10,
        divergence_detector: DivergenceDetector | None = None,
    ) -> None:
        """Initialize outbox dispatcher.

        Args:
            repository: Database repository for TradeCommand access.
            publish_fn: Async callback to publish command to ZMQ.
            poll_interval: Fallback polling interval in seconds.
            batch_size: Maximum commands per poll cycle.
            divergence_detector: Optional metric aggregator; when
                supplied, each successful dispatch increments the
                ``commands_published_durable_dispatched_total`` counter.
        """
        self._repo = repository
        self._publish_fn = publish_fn
        self._poll_interval = poll_interval
        self._batch_size = batch_size
        self._wake = asyncio.Event()
        self._running = False
        self._divergence_detector = divergence_detector

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

    async def _dispatch_batch(self) -> None:
        """Fetch and dispatch one batch of undispatched commands."""
        now = datetime.now(UTC)
        commands = await self._repo.get_undispatched_commands(as_of=now, limit=self._batch_size)
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
                if self._divergence_detector is not None:
                    self._divergence_detector.observe_command_published(
                        "durable_dispatched", cmd["exchange"], cmd["shard_key"]
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
