"""Periodic guard scanner for the paired-execution arming barrier.

DB-only liveness loop (Phase 4a) that self-heals paired-execution groups the
live ``_on_signal`` arming path could not finish:

- breaks ``assembling`` groups whose ``assembly_deadline`` has passed (a sibling
  leg never registered) — such a group never armed, so its leg commands were
  held by the outbox gate and never dispatched: no fills, nothing to flatten;
- retries arming ``assembling`` groups that are already complete but did not arm
  live (e.g. the coordinator that registered the last leg crashed before its CAS);
- cancels this coordinator's owned held ``created`` commands and terminalizes its
  owned legs for any ``broken`` group, so a sibling-broken group's held commands
  never linger and never dispatch.

Cross-coordinator: the group-break / arm CAS is global and dedup-safe (any
coordinator may win); shard-local side effects (command cancel, leg terminalize)
run ONLY for legs this coordinator owns, and run for EVERY active ``broken`` group
(not only groups this scanner just broke), so a CAS-loser still cleans up its own
legs on a later scan. Scoped to ``simultaneous`` policy; ``sequential_handoff``
(ParlayCascade) is out of scope here. Compensation of FILLED legs in a broken
ARMED group is Phase 5.
"""

import asyncio
from datetime import UTC
from datetime import datetime

from loguru import logger

from snapper.core.partitioning import ShardOwnership
from snapper.core.types import PairedExecutionGroupStatusEnum
from snapper.core.types import PairedExecutionLegStatusEnum
from snapper.core.types import PairedExecutionPolicyEnum
from snapper.core.types import TradeCommandStatusEnum
from snapper.data.repository import SQLAlchemyRepository
from snapper.data.repository_types import PairedExecutionGroupRow
from snapper.messaging.infrastructure.publisher import SequenceTracker


class PairedExecutionGuardScanner:
    """Periodic DB scanner that breaks stalled groups and retries arming.

    Attributes:
        repository: The SQL repository owning the paired-execution tables.
        ownership: Shard ownership used to scope shard-local side effects.
        interval_seconds: Seconds between scan cycles.
    """

    def __init__(
        self,
        repository: SQLAlchemyRepository,
        ownership: ShardOwnership,
        interval_seconds: float,
    ) -> None:
        """Initialize the scanner.

        Args:
            repository: The SQL repository owning the paired-execution tables.
            ownership: Shard ownership used to scope shard-local side effects.
            interval_seconds: Seconds between scan cycles.
        """
        self._repo = repository
        self._ownership = ownership
        self._interval = interval_seconds
        self._tracker = SequenceTracker()
        self._running = False

    async def run(self) -> None:
        """Run the guard scan loop until cancelled."""
        self._running = True
        logger.info(f"PairedExecutionGuardScanner started (interval={self._interval}s)")
        try:
            while True:
                await asyncio.sleep(self._interval)
                if not self._running:
                    break
                try:
                    await self._scan_cycle()
                except Exception as exc:
                    logger.warning(f"PairedExecutionGuardScanner cycle failed, will retry: {exc}")
        except asyncio.CancelledError:
            logger.info("PairedExecutionGuardScanner cancelled")
            raise
        finally:
            self._running = False

    def stop(self) -> None:
        """Signal the scan loop to stop after its current cycle."""
        self._running = False

    async def _scan_cycle(self, now: datetime | None = None) -> None:
        """Run one full scan: sweep assembling groups, then broken groups.

        Args:
            now: Scan wall-clock; defaults to ``datetime.now(UTC)`` in the
                run loop and is injectable for deterministic tests.
        """
        scan_at = now if now is not None else datetime.now(UTC)
        await self._sweep_assembling(scan_at)
        await self._sweep_broken(scan_at)

    async def _sweep_assembling(self, now: datetime) -> None:
        """Break expired assembling groups and retry arming complete ones."""
        assembling = await self._repo.list_active_paired_execution_groups(
            [PairedExecutionGroupStatusEnum.ASSEMBLING.value], now
        )
        for group in assembling:
            if group["policy"] != PairedExecutionPolicyEnum.SIMULTANEOUS.value:
                continue
            if now > group["assembly_deadline"]:
                await self._break_group(group, now)
            else:
                await self._repo.try_arm_paired_execution_group_if_complete(
                    group["public_id"],
                    now,
                    self._tracker.session_id,
                    self._tracker.next_sequence("guard.arm"),
                )

    async def _break_group(self, group: PairedExecutionGroupRow, now: datetime) -> None:
        """CAS an expired assembling group to broken (dedup-safe across instances)."""
        await self._repo.cas_paired_execution_group_status(
            group["public_id"],
            PairedExecutionGroupStatusEnum.ASSEMBLING.value,
            PairedExecutionGroupStatusEnum.BROKEN.value,
            now,
            self._tracker.session_id,
            self._tracker.next_sequence("guard.break"),
            updates={"failure_reason": "assembly timeout", "halted_at": now},
        )

    async def _sweep_broken(self, now: datetime) -> None:
        """Cancel owned held commands and terminalize owned legs of broken groups."""
        broken = await self._repo.list_active_paired_execution_groups(
            [PairedExecutionGroupStatusEnum.BROKEN.value], now
        )
        for group in broken:
            legs = await self._repo.get_paired_execution_legs(group["public_id"], now)
            for leg in legs:
                if not self._ownership.owns(leg["shard_key"]):
                    continue
                command_public_id = leg["command_public_id"]
                if command_public_id is not None:
                    await self._repo.cas_trade_command_status(
                        command_public_id,
                        TradeCommandStatusEnum.CREATED.value,
                        TradeCommandStatusEnum.CANCELLED.value,
                        now,
                        self._tracker.session_id,
                        self._tracker.next_sequence("guard.cancel"),
                        terminal_at=now,
                        last_error="paired-execution group broken",
                    )
                if leg["status"] == PairedExecutionLegStatusEnum.PENDING.value:
                    await self._repo.cas_paired_execution_leg_status(
                        leg["public_id"],
                        PairedExecutionLegStatusEnum.PENDING.value,
                        PairedExecutionLegStatusEnum.CANCELLED.value,
                        now,
                        self._tracker.session_id,
                        self._tracker.next_sequence("guard.leg"),
                    )
