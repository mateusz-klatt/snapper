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
  never linger and never dispatch;
- projects a durable halt (Phase 4b) for every active ``broken`` / ``compensating``
  group that carries REAL EXPOSURE (a leg with a non-zero ``filled_signed_qty`` or
  ``compensating`` status), and mirrors it into this coordinator's owned in-memory
  shard halts so ``_on_signal`` fast-rejects a NEW group on the same pair scope.
  Assembly-timeout breaks carry no exposure (no fills) and are NOT halted, so the
  pair stays free to re-assemble on the next tick rather than wedging forever.

Cross-coordinator: the group-break / arm CAS and the durable halt insert are global
and dedup-safe (any coordinator may win — the halt's active-unique
``(wallet, strategy, group_key)`` scope admits one); shard-local side effects
(command cancel, leg terminalize, ``halt_shard`` mirror) run ONLY for legs this
coordinator owns, and run for EVERY active ``broken`` group (not only groups this
scanner just broke), so a CAS-loser still cleans up its own legs on a later scan.
Scoped to ``simultaneous`` policy; ``sequential_handoff`` (ParlayCascade) is out of
scope here. Compensation of FILLED legs in a broken ARMED group is Phase 5; in
Phase 4b no shipped path sets ``filled_signed_qty`` on a leg, so the exposure halt
is dormant in production (unit-tested with synthetic exposed rows) until Phase 5.
"""

import asyncio
from datetime import UTC
from datetime import datetime

from loguru import logger

from snapper.application.trade.trade_service import TradeService
from snapper.core.partitioning import ShardOwnership
from snapper.core.types import PairedExecutionGroupStatusEnum
from snapper.core.types import PairedExecutionLegStatusEnum
from snapper.core.types import PairedExecutionPolicyEnum
from snapper.core.types import TradeCommandStatusEnum
from snapper.data.repository import SQLAlchemyRepository
from snapper.data.repository_types import PairedExecutionGroupRow
from snapper.data.repository_types import PairedExecutionHaltInsertRow
from snapper.data.repository_types import PairedExecutionLegRow
from snapper.messaging.infrastructure.publisher import SequenceTracker

_GROUP_BROKEN_REASON = "paired-execution group broken"


class PairedExecutionGuardScanner:
    """Periodic DB scanner that breaks stalled groups and retries arming.

    Attributes:
        repository: The SQL repository owning the paired-execution tables.
        ownership: Shard ownership used to scope shard-local side effects.
        trade_service: The in-memory trade service whose shard halts mirror
            durable paired-execution halts for the cheap ``_on_signal`` gate.
        interval_seconds: Seconds between scan cycles.
    """

    def __init__(
        self,
        repository: SQLAlchemyRepository,
        ownership: ShardOwnership,
        trade_service: TradeService,
        interval_seconds: float,
    ) -> None:
        """Initialize the scanner.

        Args:
            repository: The SQL repository owning the paired-execution tables.
            ownership: Shard ownership used to scope shard-local side effects.
            trade_service: The in-memory trade service whose owned shard halts
                mirror durable paired-execution halts.
            interval_seconds: Seconds between scan cycles.
        """
        self._repo = repository
        self._ownership = ownership
        self._trade_service = trade_service
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
        """Run one full scan: assembling, then broken, then halt projection.

        Args:
            now: Scan wall-clock; defaults to ``datetime.now(UTC)`` in the
                run loop and is injectable for deterministic tests.
        """
        scan_at = now if now is not None else datetime.now(UTC)
        await self._sweep_assembling(scan_at)
        await self._sweep_broken(scan_at)
        await self._sweep_halts(scan_at)

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
                        last_error=_GROUP_BROKEN_REASON,
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

    async def _sweep_halts(self, now: datetime) -> None:
        """Project durable halts and shard mirrors for EXPOSED broken groups.

        Lists active ``broken`` / ``compensating`` groups and halts only those
        carrying real exposure (see :meth:`_group_has_exposure`): a non-zero
        filled leg or ``compensating`` status. An assembly-timeout break has no
        fills and is skipped, so the pair stays free to re-assemble next tick.
        For a haltable group the durable halt row is inserted GLOBALLY (any
        coordinator may win the idempotent active-unique scope) and
        ``halt_shard`` is mirrored in memory ONLY for owned leg shards, matching
        the 4a global-CAS / owned-side-effect split. A ``compensating`` group
        with no legs is corruption (``mode`` is derived from a leg) — it is
        skipped and logged loudly rather than guessed.
        """
        groups = await self._repo.list_active_paired_execution_groups(
            [
                PairedExecutionGroupStatusEnum.BROKEN.value,
                PairedExecutionGroupStatusEnum.COMPENSATING.value,
            ],
            now,
        )
        for group in groups:
            legs = await self._repo.get_paired_execution_legs(group["public_id"], now)
            if not self._group_has_exposure(group, legs):
                continue
            if not legs:
                logger.warning(
                    "PairedExecutionGuardScanner: exposed group "
                    f"{group['public_id']} has no legs, skipping halt projection"
                )
                continue
            await self._repo.ensure_paired_execution_halt(self._halt_row(group, legs, now))
            for leg in legs:
                if self._ownership.owns(leg["shard_key"]):
                    self._trade_service.halt_shard(leg["shard_key"], _GROUP_BROKEN_REASON)

    def _group_has_exposure(
        self, group: PairedExecutionGroupRow, legs: list[PairedExecutionLegRow]
    ) -> bool:
        """Return whether a group carries exposure warranting a durable halt."""
        if group["status"] == PairedExecutionGroupStatusEnum.COMPENSATING.value:
            return True
        return any(abs(leg["filled_signed_qty"]) > 0.0 for leg in legs)

    def _halt_row(
        self,
        group: PairedExecutionGroupRow,
        legs: list[PairedExecutionLegRow],
        now: datetime,
    ) -> PairedExecutionHaltInsertRow:
        """Build the durable halt insert row from a group and its legs.

        ``mode`` is taken from the first leg because the group row carries no
        ``mode`` column; all legs of one group share a single mode. ``public_id``
        is omitted so the model default mints a fresh id — the active-unique
        scope, not the id, dedupes racing inserts.
        """
        return {
            "wallet_public_id": group["wallet_public_id"],
            "operator_public_id": group["operator_public_id"],
            "strategy_id": group["strategy_id"],
            "mode": legs[0]["mode"],
            "group_key": group["group_key"],
            "group_public_id": group["public_id"],
            "reason": group["failure_reason"] or _GROUP_BROKEN_REASON,
            "created_at": now,
            "session_id": self._tracker.session_id,
            "sequence_id": self._tracker.next_sequence("guard.halt"),
            "timestamp": now,
        }
