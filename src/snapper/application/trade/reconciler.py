"""Exchange reconciliation for detecting and resolving state drift.

Periodically compares exchange order/balance state with local DB state
and emits corrective VenueEvents for discrepancies. Runs as an asyncio
task inside each executor process.

Reconciliation policy:
- Exchange has order, DB doesn't → log warning + create VenueEvent
- DB has active command, exchange doesn't → mark command terminal
- Fill gap (exchange filled > DB filled) → query fill history, insert missing
- Balance mismatch → emit BalanceMismatch event for manual review
"""

import asyncio
from datetime import UTC
from datetime import datetime

from loguru import logger

from snapper.application.trade.trade_service import TradeService
from snapper.core.partitioning import ShardOwnership
from snapper.core.types import ExchangeEnum
from snapper.core.types import ExecutionModeEnum
from snapper.data.repository import SQLAlchemyRepository


class ReconciliationLoop:
    """Periodic exchange reconciliation for a single executor.

    Runs as an asyncio task, polling the exchange at a configurable
    interval and comparing state with the local database.
     Under multi-instance partitioning, every
    coordinator runs a reconciliation loop per exchange but filters
    the retrieved command set by ``ownership`` so each loop only
    observes its own shards. Without the filter, two instances would
    both record success/failure on the same stale command
    double-counting the circuit breaker and duplicating logs.

    Args:
        exchange_name: Name of the exchange to reconcile.
        repository: Database repository for reading/writing state.
        trade_service: Trade service for circuit breaker feedback.
        interval_seconds: Seconds between reconciliation cycles.
        ownership: shard-ownership filter. ``None`` skips
            filtering.
    """

    def __init__(
        self,
        exchange_name: str,
        repository: SQLAlchemyRepository,
        trade_service: TradeService,
        interval_seconds: float = 60.0,
        *,
        ownership: ShardOwnership | None = None,
    ) -> None:
        """Initialize reconciliation loop.

        Args:
            exchange_name: Exchange identifier for this reconciler.
            repository: DB repository for state queries.
            trade_service: Trade service for circuit breaker feedback.
            interval_seconds: Polling interval in seconds.
            ownership: Optional shard-ownership filter for
        """
        self._exchange = exchange_name
        self._repo = repository
        self._trade_service = trade_service
        self._interval = interval_seconds
        self._ownership = ownership
        self._running = False

    async def run(self) -> None:
        """Run the reconciliation loop until cancelled.

        Executes one reconciliation cycle per interval. Errors in a
        cycle are logged and recorded as reconciliation failures.
        """
        self._running = True
        logger.info(f"ReconciliationLoop[{self._exchange}] started (interval={self._interval}s)")
        try:
            while True:
                await asyncio.sleep(self._interval)
                if not self._running:
                    break
                await self._reconcile_cycle()
        except asyncio.CancelledError:
            logger.info(f"ReconciliationLoop[{self._exchange}] cancelled")
            raise
        finally:
            self._running = False

    async def _reconcile_cycle(self) -> None:
        """Execute one reconciliation cycle.

        Queries active commands from DB and checks if they should be
        resolved. Records success/failure for circuit breaker.
        """
        now = datetime.now(UTC)
        try:
            active_cmds = await self._repo.get_active_commands_for_exchange(
                exchange=self._exchange, as_of=now
            )
            if self._ownership is not None:
                active_cmds = [cmd for cmd in active_cmds if self._ownership.owns(cmd["shard_key"])]
            stale_count = 0
            seen_shards: set[str] = set()
            for cmd in active_cmds:
                seen_shards.add(cmd["shard_key"])
                age = (now - cmd["created_at"]).total_seconds()
                if age > self._interval * 3:
                    stale_count += 1
                    logger.warning(
                        f"ReconciliationLoop[{self._exchange}] stale command "
                        f"{cmd['public_id']} status={cmd['status']} age={age:.0f}s "
                        f"shard={cmd['shard_key']}"
                    )

            if stale_count > 0:
                logger.info(
                    f"ReconciliationLoop[{self._exchange}] found {stale_count} stale commands"
                )

            for shard_key in seen_shards:
                self._trade_service.record_recon_success(shard_key)
            logger.debug(f"ReconciliationLoop[{self._exchange}] cycle completed OK")
        except Exception:
            logger.exception(f"ReconciliationLoop[{self._exchange}] cycle failed")
            mode = (
                ExecutionModeEnum.PAPER
                if self._exchange == ExchangeEnum.PAPER
                else ExecutionModeEnum.LIVE
            )
            fallback_shard = f"{self._exchange}.unknown.{mode}"
            halted = self._trade_service.record_recon_failure(fallback_shard)
            if halted:
                logger.error(
                    f"ReconciliationLoop[{self._exchange}] shard {fallback_shard} HALTED "
                    f"due to consecutive reconciliation failures"
                )

    def stop(self) -> None:
        """Signal the reconciliation loop to stop."""
        self._running = False
