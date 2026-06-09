"""Periodic guard scanner for the paired-execution arming barrier.

DB-only liveness loop (Phase 4a) that self-heals paired-execution groups the
live ``_on_signal`` arming path could not finish:

- breaks ``assembling`` groups whose ``assembly_deadline`` has passed (a sibling
  leg never registered) — such a group never armed, so its leg commands were
  held by the outbox gate and never dispatched: no fills, nothing to flatten;
- retries arming ``assembling`` groups that are already complete but did not arm
  live (e.g. the coordinator that registered the last leg crashed before its CAS);
- breaks ``armed`` groups whose ``fill_deadline`` passed without every leg fully
  filling, or that have a leg in a terminal-without-fill state (a venue reject /
  cancel / expire projected onto the leg by the live terminal hook) — so a group
  whose sibling can never complete stops holding the filled sibling exposed
  (Phase 5b breaks + halts; Phase 5c flattens the exposure);
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
from uuid import uuid7

from loguru import logger

from snapper.application.risk.models import RiskEvaluator
from snapper.application.trade.outbox import OutboxDispatcher
from snapper.application.trade.trade_service import TradeService
from snapper.core.partitioning import ShardOwnership
from snapper.core.types import PairedExecutionGroupStatusEnum
from snapper.core.types import PairedExecutionLegStatusEnum
from snapper.core.types import PairedExecutionPolicyEnum
from snapper.core.types import TradeCommandStatusEnum
from snapper.core.types import TradeSideEnum
from snapper.data.repository import SQLAlchemyRepository
from snapper.data.repository_types import PairedExecutionGroupRow
from snapper.data.repository_types import PairedExecutionHaltInsertRow
from snapper.data.repository_types import PairedExecutionLegRow
from snapper.data.repository_types import TradeCommandInsertRow
from snapper.messaging.infrastructure.publisher import SequenceTracker

_GROUP_BROKEN_REASON = "paired-execution group broken"
_LEG_TERMINAL_NO_FILL_STATUSES = frozenset(
    {
        PairedExecutionLegStatusEnum.REJECTED.value,
        PairedExecutionLegStatusEnum.CANCELLED.value,
        PairedExecutionLegStatusEnum.EXPIRED.value,
    }
)
_ARMED_FILL_TIMEOUT_REASON = "fill timeout"
_ARMED_LEG_TERMINAL_REASON = "leg terminal before fill"
_QTY_EPSILON = 1e-12
_GROUP_ALWAYS_HALTED_STATUSES = frozenset(
    {
        PairedExecutionGroupStatusEnum.COMPENSATING.value,
        PairedExecutionGroupStatusEnum.MANUAL_INTERVENTION.value,
    }
)
_LIVE_COMMAND_STATUSES = frozenset(
    {
        TradeCommandStatusEnum.DISPATCHED.value,
        TradeCommandStatusEnum.DIRECT_DISPATCHED.value,
        TradeCommandStatusEnum.ACCEPTED.value,
        TradeCommandStatusEnum.PARTIALLY_FILLED.value,
    }
)
_LEG_CANCEL_ELIGIBLE_STATUSES = frozenset(
    {
        PairedExecutionLegStatusEnum.PENDING.value,
        PairedExecutionLegStatusEnum.ARMED.value,
        PairedExecutionLegStatusEnum.WORKING.value,
        PairedExecutionLegStatusEnum.PARTIALLY_FILLED.value,
    }
)
_LEG_FLATTEN_ELIGIBLE_STATUSES = frozenset(
    {
        PairedExecutionLegStatusEnum.FILLED.value,
        PairedExecutionLegStatusEnum.CANCELLED.value,
        PairedExecutionLegStatusEnum.EXPIRED.value,
        PairedExecutionLegStatusEnum.REJECTED.value,
    }
)


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
        outbox: OutboxDispatcher | None = None,
    ) -> None:
        """Initialize the scanner.

        Args:
            repository: The SQL repository owning the paired-execution tables.
            ownership: Shard ownership used to scope shard-local side effects.
            trade_service: The in-memory trade service whose owned shard halts
                mirror durable paired-execution halts.
            interval_seconds: Seconds between scan cycles.
            outbox: The durable-command outbox dispatcher, notified after a
                compensation command is emitted so it dispatches promptly
                (None in tests / no-SQL setups; the outbox poll loop still
                picks the command up on its next tick).
        """
        self._repo = repository
        self._ownership = ownership
        self._trade_service = trade_service
        self._interval = interval_seconds
        self._outbox = outbox
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
        """Run one scan: assembling, armed, broken, compensating, halt projection.

        The ``armed`` sweep runs AFTER ``assembling`` and BEFORE ``broken`` so a
        group broken this tick for a fill timeout or a terminal leg is cleaned up
        within the SAME cycle. ``compensating`` runs AFTER ``broken`` (so the held
        original commands are already cancelled) and BEFORE ``halts`` (so a group
        moved to ``compensating`` is halted the same cycle).

        Args:
            now: Scan wall-clock; defaults to ``datetime.now(UTC)`` in the
                run loop and is injectable for deterministic tests.
        """
        scan_at = now if now is not None else datetime.now(UTC)
        await self._sweep_assembling(scan_at)
        await self._sweep_armed(scan_at)
        await self._sweep_broken(scan_at)
        await self._sweep_compensating(scan_at)
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

    async def _sweep_armed(self, now: datetime) -> None:
        """Break armed groups that timed out filling or have a terminal leg.

        For each active ``armed`` ``simultaneous`` group, breaks it (``armed`` →
        ``broken``, dedup-safe CAS) when EITHER a leg went terminal without a
        full fill (a venue reject / cancel / expire projected onto the leg by the
        live terminal hook) OR the group's ``fill_deadline`` passed with at least
        one leg not fully ``filled``. A terminal leg is broken immediately (before
        the deadline) because one sibling can never complete, so holding the
        already-filled sibling exposed until the deadline is avoidable risk. The
        break only flips the group status + records the reason; cancelling live
        orders and flattening filled legs is Phase 5c — here the same cycle's
        later ``broken`` / ``halt`` sweeps cancel owned held commands and (for an
        exposed break) project the durable halt.
        """
        armed = await self._repo.list_active_paired_execution_groups(
            [PairedExecutionGroupStatusEnum.ARMED.value], now
        )
        for group in armed:
            if group["policy"] != PairedExecutionPolicyEnum.SIMULTANEOUS.value:
                continue
            legs = await self._repo.get_paired_execution_legs(group["public_id"], now)
            reason = self._armed_break_reason(group, legs, now)
            if reason is not None:
                await self._break_armed_group(group, now, reason)

    def _armed_break_reason(
        self,
        group: PairedExecutionGroupRow,
        legs: list[PairedExecutionLegRow],
        now: datetime,
    ) -> str | None:
        """Return why an armed group must break, or ``None`` to leave it armed.

        A leg in a terminal-without-fill state (rejected / cancelled / expired)
        breaks the group at once. Otherwise the group breaks only once its
        ``fill_deadline`` has passed while at least one leg is not fully
        ``filled``. A complete set of fully filled legs (the success path) and a
        not-yet-expired group with all legs still working both stay armed. An
        empty leg set (corruption) carries no exposure and is left for the
        assembling-stage validation rather than broken here.
        """
        if not legs:
            return None
        if any(leg["status"] in _LEG_TERMINAL_NO_FILL_STATUSES for leg in legs):
            return _ARMED_LEG_TERMINAL_REASON
        if now > group["fill_deadline"] and not all(
            leg["status"] == PairedExecutionLegStatusEnum.FILLED.value for leg in legs
        ):
            return _ARMED_FILL_TIMEOUT_REASON
        return None

    async def _break_armed_group(
        self, group: PairedExecutionGroupRow, now: datetime, reason: str
    ) -> None:
        """CAS an armed group to broken with a failure reason (dedup-safe)."""
        await self._repo.cas_paired_execution_group_status(
            group["public_id"],
            PairedExecutionGroupStatusEnum.ARMED.value,
            PairedExecutionGroupStatusEnum.BROKEN.value,
            now,
            self._tracker.session_id,
            self._tracker.next_sequence("guard.armed.break"),
            updates={"failure_reason": reason, "halted_at": now},
        )

    async def _sweep_broken(self, now: datetime) -> None:
        """Cancel owned held commands and terminalize owned never-dispatched legs.

        A PENDING leg is force-terminalized to ``cancelled`` ONLY when its
        original command was a HELD ``created`` row (the cancel CAS returned
        True) or it has no command — i.e. the order NEVER reached the venue.
        A PENDING leg whose command already dispatched is left PENDING: its
        venue order may still be live, so the compensation sweep cancels it at
        the venue and lets the venue terminal (or a fill) drive the leg status,
        rather than prematurely marking it ``cancelled`` (which would make a
        late fill invisible to the fill projection).

        Covers BOTH ``broken`` and ``compensating`` groups: once one coordinator
        moves a group to ``compensating`` for a live original on its shard, the
        held-command / never-dispatched-leg cleanup for a SIBLING coordinator's
        owned legs must still run, or a held-``created`` / no-command leg owned
        by that sibling would linger ``pending`` forever.
        """
        broken = await self._repo.list_active_paired_execution_groups(
            [
                PairedExecutionGroupStatusEnum.BROKEN.value,
                PairedExecutionGroupStatusEnum.COMPENSATING.value,
            ],
            now,
        )
        for group in broken:
            legs = await self._repo.get_paired_execution_legs(group["public_id"], now)
            for leg in legs:
                if not self._ownership.owns(leg["shard_key"]):
                    continue
                command_public_id = leg["command_public_id"]
                cancelled_held = False
                if command_public_id is not None:
                    cancelled_held = await self._repo.cas_trade_command_status(
                        command_public_id,
                        TradeCommandStatusEnum.CREATED.value,
                        TradeCommandStatusEnum.CANCELLED.value,
                        now,
                        self._tracker.session_id,
                        self._tracker.next_sequence("guard.cancel"),
                        terminal_at=now,
                        last_error=_GROUP_BROKEN_REASON,
                    )
                never_dispatched = command_public_id is None or cancelled_held
                if leg["status"] == PairedExecutionLegStatusEnum.PENDING.value and never_dispatched:
                    await self._repo.cas_paired_execution_leg_status(
                        leg["public_id"],
                        PairedExecutionLegStatusEnum.PENDING.value,
                        PairedExecutionLegStatusEnum.CANCELLED.value,
                        now,
                        self._tracker.session_id,
                        self._tracker.next_sequence("guard.leg"),
                    )

    async def _sweep_compensating(self, now: datetime) -> None:
        """Cancel live originals (5c.1) and flatten terminal exposure (5c.2).

        Each OWNED leg is routed by status: a non-terminal leg
        (:data:`_LEG_CANCEL_ELIGIBLE_STATUSES`) with a live original is cancelled
        at the venue (5c.1, :meth:`_cancel_live_original`); a venue-terminal leg
        (:data:`_LEG_FLATTEN_ELIGIBLE_STATUSES` — filled / cancelled / expired /
        rejected, where the original can no longer fill) with residual
        ``open_group_qty`` is flattened with a reduce-only MARKET order (5c.2,
        :meth:`_flatten_leg`). Already-handled legs (compensating / flattened /
        manual_intervention) fall through untouched.

        Phase 5c.1 (cancel): for each active ``broken`` / ``compensating``
        ``simultaneous`` group, for each OWNED leg still in a non-terminal status
        (:data:`_LEG_CANCEL_ELIGIBLE_STATUSES`) whose ORIGINAL command is still
        LIVE at the venue (command status in :data:`_LIVE_COMMAND_STATUSES`), the
        group is moved ``broken`` → ``compensating`` (dedup-safe CAS) and an
        idempotent venue cancel command is emitted for the original order. BOTH
        guards are required: leg status alone is not a venue-liveness signal (a
        dispatched leg stays ``pending`` until a fill / venue terminal is
        projected), and command status alone is not either (venue terminals are
        projected onto the LEG, not back onto the command, so a FILLED / CANCELLED
        leg can still read command status ``dispatched``). The leg is NOT claimed
        or flattened here — it stays fill-projectable until the venue cancel /
        expire / reject (or a fill) terminalizes it; Phase 5c.2 then flattens any
        residual exposure. Lists ``broken`` AND ``compensating`` so a CAS loser, a
        re-scan, or a leg that turns live after the group flipped is still
        handled. Scoped to ``simultaneous`` policy.
        """
        groups = await self._repo.list_active_paired_execution_groups(
            [
                PairedExecutionGroupStatusEnum.BROKEN.value,
                PairedExecutionGroupStatusEnum.COMPENSATING.value,
            ],
            now,
        )
        for group in groups:
            if group["policy"] != PairedExecutionPolicyEnum.SIMULTANEOUS.value:
                continue
            legs = await self._repo.get_paired_execution_legs(group["public_id"], now)
            for leg in legs:
                if not self._ownership.owns(leg["shard_key"]):
                    continue
                status = leg["status"]
                if status in _LEG_CANCEL_ELIGIBLE_STATUSES:
                    await self._cancel_live_original(group, leg, now)
                elif status in _LEG_FLATTEN_ELIGIBLE_STATUSES:
                    await self._flatten_leg(group, leg, now)
                elif status == PairedExecutionLegStatusEnum.COMPENSATING.value:
                    await self._settle_compensating_leg(leg, now)

    async def _settle_compensating_leg(self, leg: PairedExecutionLegRow, now: datetime) -> None:
        """Backstop: re-derive an owned compensating leg's settlement from venue_events.

        Phase 5d.2's live terminal hook settles a flatten order's cancel / expire /
        reject the instant the message arrives, but two cases never reach it: a
        venue cancel / expire recorded durably with NO trader terminal message, and
        a not-tradeable reject that publishes without recording a row (so the live
        recompute could miss terminality). Each scan this reprojects the leg's
        compensation from the AUTHORITATIVE ``venue_events`` — idempotent (a
        ``COMPENSATION_NOOP`` while the flatten is still in flight), and on a
        durable terminal it FLATTENS the leg (residual gone) or reopens it to
        ``filled`` (residual remains) so the next sweep re-flattens. A reopened leg
        is dispatched by the next cycle's flatten claim, so no outbox notify is
        needed here.
        """
        await self._repo.reproject_paired_execution_leg_compensation(
            leg["public_id"],
            now,
            self._tracker.session_id,
            self._tracker.next_sequence("guard.comp.settle"),
        )

    async def _cancel_live_original(
        self,
        group: PairedExecutionGroupRow,
        leg: PairedExecutionLegRow,
        now: datetime,
    ) -> None:
        """Emit an idempotent venue cancel for one owned leg with a live original.

        No-op unless the leg binds a command whose status is venue-live. On a live
        original the group is moved to ``compensating`` (so :meth:`_sweep_halts`
        halts the pair this cycle even at zero exposure) and a
        ``command_type='cancel'`` command is inserted with an
        ``idempotency_key`` that dedups re-emission across cycles and instances;
        the ``supersedes_command_id`` lets the broken-group outbox gate release
        it. The outbox is notified only when a command was actually inserted.
        """
        command_public_id = leg["command_public_id"]
        client_order_id = leg["client_order_id"]
        if command_public_id is None or client_order_id is None:
            return
        command_status = await self._repo.get_current_trade_command_status(command_public_id)
        if command_status not in _LIVE_COMMAND_STATUSES:
            return
        await self._ensure_group_compensating(group, now)
        inserted = await self._repo.insert_paired_compensation_command(
            self._paired_cancel_row(group, leg, client_order_id, command_public_id, now)
        )
        if inserted is not None and self._outbox is not None:
            self._outbox.notify()

    async def _ensure_group_compensating(
        self, group: PairedExecutionGroupRow, now: datetime
    ) -> None:
        """CAS a broken group to compensating (dedup-safe; no-op if already so)."""
        if group["status"] != PairedExecutionGroupStatusEnum.BROKEN.value:
            return
        await self._repo.cas_paired_execution_group_status(
            group["public_id"],
            PairedExecutionGroupStatusEnum.BROKEN.value,
            PairedExecutionGroupStatusEnum.COMPENSATING.value,
            now,
            self._tracker.session_id,
            self._tracker.next_sequence("guard.compensate"),
        )

    def _paired_cancel_row(
        self,
        group: PairedExecutionGroupRow,
        leg: PairedExecutionLegRow,
        client_order_id: str,
        command_public_id: str,
        now: datetime,
    ) -> TradeCommandInsertRow:
        """Build the venue cancel command row for a leg's live original order.

        ``client_order_id`` / ``venue_client_id`` target the ORIGINAL order so the
        executor cancels it at the venue; ``supersedes_command_id`` is the
        original command id (releasing the broken-group outbox gate); the
        ``idempotency_key`` is unique per ``(group, leg)`` cancel so re-emission
        across scan cycles is deduped by the active-unique index. The leg's
        ``exchange_order_id`` is carried when known (a partially-filled leg has
        one) so the cancel dispatch does not depend on the orders projection to
        hydrate the venue id.
        """
        return {
            "command_type": "cancel",
            "shard_key": leg["shard_key"],
            "exchange": leg["exchange"],
            "instrument": leg["instrument"],
            "mode": leg["mode"],
            "strategy_id": group["strategy_id"],
            "client_order_id": client_order_id,
            "venue_client_id": client_order_id,
            "side": leg["side"],
            "order_type": "market",
            "quantity": leg["target_qty"],
            "price": None,
            "reduce_only": False,
            "status": TradeCommandStatusEnum.CREATED.value,
            "created_at": now,
            "correlation_id": group["public_id"],
            "session_id": self._tracker.session_id,
            "sequence_id": self._tracker.next_sequence("guard.cancel.cmd"),
            "timestamp": now,
            "idempotency_key": f"paired:{group['public_id']}:{leg['public_id']}:cancel",
            "supersedes_command_id": command_public_id,
            "exchange_order_id": leg["exchange_order_id"],
            "wallet_public_id": leg["wallet_public_id"],
            "operator_public_id": leg["operator_public_id"],
            "source_surface": "strategy",
        }

    async def _flatten_leg(
        self,
        group: PairedExecutionGroupRow,
        leg: PairedExecutionLegRow,
        now: datetime,
    ) -> None:
        """Flatten one owned venue-terminal leg's residual exposure (Phase 5c.2).

        ``open_group_qty = filled_signed_qty − compensated_signed_qty``. A zero
        residual is a no-op (nothing filled, or already compensated — completion
        is Phase 5d). Otherwise the flatten quantity is resolved (capability +
        spec + lot rounding); an unresolvable leg (no instrument id, no
        reduce-only support, missing spec, sub-lot dust, or below the venue lot
        minimum) escalates the leg AND group to ``manual_intervention`` rather
        than emit an order that could under- or over-flatten. A resolvable leg is
        flattened with a SIGN-based reduce-only MARKET order (net long → sell,
        net short → buy) via the atomic claim-leg-and-insert DAL, keeping the leg
        bound to its ORIGINAL command. The outbox is notified on a successful
        claim. The group is moved to ``compensating`` first so the pair stays
        halted while compensation runs.
        """
        open_qty = leg["filled_signed_qty"] - leg["compensated_signed_qty"]
        if abs(open_qty) < _QTY_EPSILON:
            return
        instrument_public_id = await self._repo.get_instrument_public_id_by_symbol(
            leg["instrument"], leg["exchange"], now
        )
        flatten_qty = await self._resolve_flatten_qty(leg, instrument_public_id, open_qty, now)
        if flatten_qty is None:
            await self._escalate_to_manual(group, leg, now)
            return
        await self._ensure_group_compensating(group, now)
        new_seq = leg["compensation_seq"] + 1
        side = TradeSideEnum.SELL.value if open_qty > 0 else TradeSideEnum.BUY.value
        inserted = await self._repo.claim_leg_and_insert_flatten_command(
            leg_public_id=leg["public_id"],
            expected_status=leg["status"],
            new_compensation_seq=new_seq,
            command_row=self._flatten_command_row(group, leg, side, flatten_qty, new_seq, now),
            bus_time=now,
            session_id=self._tracker.session_id,
            sequence_id=self._tracker.next_sequence("guard.flatten"),
        )
        if inserted is not None and self._outbox is not None:
            self._outbox.notify()

    async def _resolve_flatten_qty(
        self,
        leg: PairedExecutionLegRow,
        instrument_public_id: str | None,
        open_qty: float,
        now: datetime,
    ) -> float | None:
        """Return the lot-rounded reduce-only flatten qty, or None to escalate.

        Returns None (→ manual_intervention) when the instrument cannot be
        resolved, the venue does not support reduce-only orders (sending a plain
        order could OPEN fresh opposite exposure), the instrument spec / lot size
        is missing, or the residual rounds below one lot / the venue minimum
        order size (over-flattening past the open qty would open opposite
        exposure, so dust is left for an operator).
        """
        if instrument_public_id is None:
            return None
        caps = await self._repo.get_instrument_capabilities(
            now, exchange=leg["exchange"], instrument_public_id=instrument_public_id
        )
        if not caps or not caps[0]["supports_reduce_only"]:
            return None
        spec = await self._repo.get_instrument_spec(instrument_public_id, now)
        if spec is None:
            return None
        lot_size = spec["lot_size"]
        if lot_size is None or lot_size <= 0.0:
            return None
        flatten_qty = RiskEvaluator.round_down_to_step(abs(open_qty), lot_size)
        min_order_size = spec["min_order_size"]
        if flatten_qty <= 0.0 or (min_order_size is not None and flatten_qty < min_order_size):
            return None
        return flatten_qty

    async def _escalate_to_manual(
        self,
        group: PairedExecutionGroupRow,
        leg: PairedExecutionLegRow,
        now: datetime,
    ) -> None:
        """Escalate one leg and its group to ``manual_intervention``.

        The leg cannot be auto-flattened safely (no reduce-only support, no
        spec, or sub-lot dust), so an operator must intervene. The group is
        escalated ONLY IF this coordinator actually transitioned the leg: a
        failed leg CAS means another instance already claimed the leg for a real
        flatten (moved it to ``compensating``) between this scanner's read and
        its escalate, so forcing the group to ``manual_intervention`` would
        falsely flag a pair whose reduce-only flatten is already in flight. When
        the leg CAS wins, the group CAS is attempted from both ``broken`` and
        ``compensating`` (a sibling leg may already have moved it to
        ``compensating`` this cycle); one transition wins.
        """
        leg_escalated = await self._repo.cas_paired_execution_leg_status(
            leg["public_id"],
            leg["status"],
            PairedExecutionLegStatusEnum.MANUAL_INTERVENTION.value,
            now,
            self._tracker.session_id,
            self._tracker.next_sequence("guard.manual.leg"),
        )
        if not leg_escalated:
            return
        for expected in (
            PairedExecutionGroupStatusEnum.BROKEN.value,
            PairedExecutionGroupStatusEnum.COMPENSATING.value,
        ):
            escalated = await self._repo.cas_paired_execution_group_status(
                group["public_id"],
                expected,
                PairedExecutionGroupStatusEnum.MANUAL_INTERVENTION.value,
                now,
                self._tracker.session_id,
                self._tracker.next_sequence("guard.manual.grp"),
            )
            if escalated:
                break

    def _flatten_command_row(
        self,
        group: PairedExecutionGroupRow,
        leg: PairedExecutionLegRow,
        side: str,
        quantity: float,
        compensation_seq: int,
        now: datetime,
    ) -> TradeCommandInsertRow:
        """Build the reduce-only MARKET flatten command for one leg.

        A FRESH ``client_order_id`` / ``venue_client_id`` (this is a NEW venue
        order, not the original); ``reduce_only=True`` so it can only reduce
        exposure; ``supersedes_command_id`` is the leg's ORIGINAL command (its
        flatten fills route back to the leg by supersedes in Phase 5d, and the
        broken-group outbox gate releases it); the ``idempotency_key`` carries
        the ``compensation_seq`` so each compensation round is a distinct,
        re-run-safe command.
        """
        order_id = str(uuid7())
        return {
            "command_type": "submit",
            "shard_key": leg["shard_key"],
            "exchange": leg["exchange"],
            "instrument": leg["instrument"],
            "mode": leg["mode"],
            "strategy_id": group["strategy_id"],
            "client_order_id": order_id,
            "venue_client_id": order_id,
            "side": side,
            "order_type": "market",
            "quantity": quantity,
            "price": None,
            "reduce_only": True,
            "status": TradeCommandStatusEnum.CREATED.value,
            "created_at": now,
            "correlation_id": group["public_id"],
            "session_id": self._tracker.session_id,
            "sequence_id": self._tracker.next_sequence("guard.flatten.cmd"),
            "timestamp": now,
            "idempotency_key": (
                f"paired:{group['public_id']}:{leg['public_id']}:flatten:{compensation_seq}"
            ),
            "supersedes_command_id": leg["command_public_id"],
            "wallet_public_id": leg["wallet_public_id"],
            "operator_public_id": leg["operator_public_id"],
            "source_surface": "strategy",
        }

    async def _sweep_halts(self, now: datetime) -> None:
        """Project durable halts and shard mirrors for EXPOSED broken groups.

        Lists active ``broken`` / ``compensating`` / ``manual_intervention``
        groups and halts only those carrying real exposure (see
        :meth:`_group_has_exposure`): a non-zero filled leg, or a
        ``compensating`` / ``manual_intervention`` status. An assembly-timeout
        break has no fills and is skipped, so the pair stays free to re-assemble
        next tick. ``manual_intervention`` is included because a leg that cannot
        be auto-flattened (Phase 5c.2) leaves real exposure that MUST stay halted
        until an operator resolves it. For a haltable group the durable halt row
        is inserted GLOBALLY (any coordinator may win the idempotent active-unique
        scope) and ``halt_shard`` is mirrored in memory ONLY for owned leg shards,
        matching the 4a global-CAS / owned-side-effect split. A group with no legs
        is corruption (``mode`` is derived from a leg) — it is skipped and logged
        loudly rather than guessed.
        """
        groups = await self._repo.list_active_paired_execution_groups(
            [
                PairedExecutionGroupStatusEnum.BROKEN.value,
                PairedExecutionGroupStatusEnum.COMPENSATING.value,
                PairedExecutionGroupStatusEnum.MANUAL_INTERVENTION.value,
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
        if group["status"] in _GROUP_ALWAYS_HALTED_STATUSES:
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
