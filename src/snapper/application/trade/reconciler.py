"""Exchange reconciliation loop for active trade-command health.

Periodically scans active DB commands for one exchange, folds the
append-only ``venue_events`` truth plane into durable ``TradeCommand``
status advances, logs genuinely stale rows, and feeds reconciliation
success/failure counters into :class:`TradeService`. Runs as an
asyncio task inside the coordinator (one loop per exchange).

Lifecycle fold policy (#145 P2-4):
- ``venue_events`` stays the truth; ``trade_commands`` becomes a fold
  of it. The fold is rank-monotonic — a command status never regresses
  — and only touches non-terminal create/submit commands past the
  outbox's territory (``dispatched``/``direct_dispatched``/
  ``accepted``/``partially_filled``); ``created`` rows belong to the
  outbox and guard scanner, keeping the CAS expected-status sets
  disjoint by construction.
- A lost CAS is skip-and-next-cycle, never retry-in-cycle.

Stale reporting policy:
- Active commands older than three intervals WARN only when they have
  ZERO venue evidence (true anomalies the executor's verification
  sweep will work); unknown-only evidence reports at INFO (the sweep
  is already on it); commands with real evidence are silent here —
  the fold advances them, and an open limit order is healthy, not
  stale.
- A successful scan clears the failure counter for every shard seen in
  the active command set.
- A scan exception records reconciliation failures only on real shard
  keys known to the coordinator or seen by a prior successful cycle;
  repeated failures halt those shards via :class:`TradeService`.
"""

import asyncio
from dataclasses import dataclass
from datetime import UTC
from datetime import datetime

from loguru import logger

from snapper.application.trade.command_request import parse_shard_key
from snapper.application.trade.trade_service import TradeService
from snapper.core.partitioning import ShardOwnership
from snapper.core.types import TradeCommandStatusEnum
from snapper.data.repository import SQLAlchemyRepository
from snapper.data.repository_types import TradeCommandRow
from snapper.data.repository_types import VenueEventRow

_FOLD_COMMAND_TYPES: tuple[str, ...] = ("create", "submit")
_FOLD_SOURCE_STATUSES: frozenset[str] = frozenset(
    {
        TradeCommandStatusEnum.DISPATCHED,
        TradeCommandStatusEnum.DIRECT_DISPATCHED,
        TradeCommandStatusEnum.ACCEPTED,
        TradeCommandStatusEnum.PARTIALLY_FILLED,
    }
)
_STATUS_RANK: dict[str, int] = {
    TradeCommandStatusEnum.CREATED: 0,
    TradeCommandStatusEnum.DISPATCHED: 1,
    TradeCommandStatusEnum.DIRECT_DISPATCHED: 1,
    TradeCommandStatusEnum.ACCEPTED: 2,
    TradeCommandStatusEnum.PARTIALLY_FILLED: 3,
    TradeCommandStatusEnum.FILLED: 4,
    TradeCommandStatusEnum.REJECTED: 4,
    TradeCommandStatusEnum.CANCELLED: 4,
    TradeCommandStatusEnum.EXPIRED: 4,
    TradeCommandStatusEnum.FAILED: 4,
}
_TERMINAL_EVENT_STATUS_MAP: dict[str, TradeCommandStatusEnum] = {
    "filled": TradeCommandStatusEnum.FILLED,
    "closed": TradeCommandStatusEnum.FILLED,
    "canceled": TradeCommandStatusEnum.CANCELLED,
    "cancelled": TradeCommandStatusEnum.CANCELLED,
    "expired": TradeCommandStatusEnum.EXPIRED,
}
_FILL_COMPLETE_REL_TOL = 1e-9


@dataclass(frozen=True)
class _LifecycleAdvance:
    """Computed fold target for one command's durable status advance."""

    status: TradeCommandStatusEnum
    acked_at: datetime | None
    exchange_order_id: str | None
    terminal_at: datetime | None
    last_error: str | None


@dataclass
class _LifecycleFoldState:
    """Mutable accumulator for one command's venue lifecycle fold."""

    acked_at: datetime | None = None
    exchange_order_id: str | None = None
    max_cum: float | None = None
    fill_complete: bool = False
    terminal_status: TradeCommandStatusEnum | None = None
    terminal_at: datetime | None = None
    last_error: str | None = None

    def ingest(self, event: VenueEventRow) -> None:
        """Apply one venue event to the current fold state."""
        event_type = event["event_type"]
        if event_type == "order_accepted":
            self._ingest_accept(event)
        elif event_type == "fill_observed":
            self._ingest_fill(event)
        elif event_type == "order_rejected":
            self._ingest_rejection(event)
        elif event_type == "order_breaker_open":
            self._ingest_breaker_open(event)
        elif event_type == "order_terminal":
            self._ingest_terminal(event)

    def to_advance(self, cmd: TradeCommandRow) -> _LifecycleAdvance | None:
        """Return the durable status advance represented by this fold."""
        target = self._target_status(cmd["quantity"])
        if target is None:
            return None
        if _STATUS_RANK[target] <= _STATUS_RANK.get(cmd["status"], 0):
            return None
        terminal_at, last_error = self._terminal_fields()
        return _LifecycleAdvance(
            status=target,
            acked_at=self.acked_at,
            exchange_order_id=self.exchange_order_id,
            terminal_at=terminal_at,
            last_error=last_error,
        )

    def _ingest_accept(self, event: VenueEventRow) -> None:
        """Fold an order acceptance observation."""
        self._clear_rejection()
        if self.acked_at is None:
            self.acked_at = event["received_at"]
        self._replace_exchange_order_id(event["exchange_order_id"])

    def _ingest_fill(self, event: VenueEventRow) -> None:
        """Fold a fill observation."""
        self._clear_rejection()
        self.max_cum = _max_cumulative_fill(self.max_cum, event["cum_fill_size"])
        self.fill_complete = self.fill_complete or event["status"] == "filled"
        if self.exchange_order_id is None:
            self._replace_exchange_order_id(event["exchange_order_id"])

    def _ingest_rejection(self, event: VenueEventRow) -> None:
        """Fold a venue rejection observation."""
        self.terminal_status = TradeCommandStatusEnum.REJECTED
        self.terminal_at = event["received_at"]
        self.last_error = event["error"] or "rejected by venue"

    def _ingest_breaker_open(self, event: VenueEventRow) -> None:
        """Fold a local circuit-breaker rejection observation."""
        self.terminal_status = TradeCommandStatusEnum.FAILED
        self.terminal_at = event["received_at"]
        self.last_error = "circuit_breaker_open"

    def _ingest_terminal(self, event: VenueEventRow) -> None:
        """Fold a terminal venue lifecycle observation."""
        raw_status = (event["status"] or "").lower()
        mapped = _TERMINAL_EVENT_STATUS_MAP.get(raw_status)
        self.terminal_status = mapped or TradeCommandStatusEnum.CANCELLED
        self.terminal_at = event["received_at"]
        self.last_error = None if mapped else f"unmapped terminal status {raw_status!r}"

    def _clear_rejection(self) -> None:
        """Clear an earlier REJECTED target once later live evidence lands."""
        if self.terminal_status is TradeCommandStatusEnum.REJECTED:
            self.terminal_status = None
            self.terminal_at = None
            self.last_error = None

    def _replace_exchange_order_id(self, exchange_order_id: str | None) -> None:
        """Store a non-empty exchange order id."""
        if exchange_order_id:
            self.exchange_order_id = exchange_order_id

    def _target_status(self, quantity: float) -> TradeCommandStatusEnum | None:
        """Resolve the best durable command status from accumulated evidence."""
        if self.terminal_status is not None:
            return self.terminal_status
        if self.max_cum is not None and self.max_cum > 0:
            return self._fill_target(quantity)
        if self.acked_at is not None:
            return TradeCommandStatusEnum.ACCEPTED
        return None

    def _fill_target(self, quantity: float) -> TradeCommandStatusEnum:
        """Resolve partial-vs-full fill state."""
        filled_completely = self.fill_complete or (
            bool(quantity)
            and self.max_cum is not None
            and self.max_cum >= quantity * (1 - _FILL_COMPLETE_REL_TOL)
        )
        if filled_completely:
            return TradeCommandStatusEnum.FILLED
        return TradeCommandStatusEnum.PARTIALLY_FILLED

    def _terminal_fields(self) -> tuple[datetime | None, str | None]:
        """Return terminal payload only when the target is terminal."""
        if self.terminal_status is not None:
            return self.terminal_at, self.last_error
        return None, None


def _max_cumulative_fill(current: float | None, candidate: float | None) -> float | None:
    """Return the greater known cumulative fill value."""
    if candidate is None:
        return current
    if current is None:
        return candidate
    return max(current, candidate)


def _fold_lifecycle_advance(
    cmd: TradeCommandRow, events: list[VenueEventRow]
) -> _LifecycleAdvance | None:
    """Fold a command's venue events into its target durable status.

    Walks the events in observation order (``id`` ascending). The FIRST
    ``order_accepted`` supplies ``acked_at``; the LAST terminal-class
    event wins (most recent venue truth); the MAX cumulative fill
    decides partial-vs-full, with the executor-written fill status
    ``filled`` taken as authoritative completeness (the executor's
    fill-complete tolerance is looser than any cum-vs-quantity check
    here could safely be). ``order_submit_unknown`` rows deliberately
    advance nothing — the executor's verification sweep owns those.
    A rejection is NOT forever: ``order_rejected`` is excluded from
    duplicate-submit evidence precisely so the outbox may retry the
    command, so LATER accepted/fill evidence supersedes an earlier
    rejection (the retry reached the venue); other terminal classes
    have no legal retry path and are never superseded. Returns
    ``None`` when there is no advance (no evidence, or the target does
    not outrank the current status) so duplicate and out-of-order rows
    collapse idempotently.

    Args:
        cmd: Active command row being folded.
        events: The command's lifecycle venue events, ordered by id.

    Returns:
        The advance to apply, or ``None`` for no-op.
    """
    state = _LifecycleFoldState()
    for event in events:
        state.ingest(event)
    return state.to_advance(cmd)


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
                multi-instance coordinators.
        """
        self._exchange = exchange_name
        self._repo = repository
        self._trade_service = trade_service
        self._interval = interval_seconds
        self._ownership = ownership
        self._running = False
        self._last_seen_shards: set[str] = set()

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

    async def _advance_command(
        self, cmd: TradeCommandRow, advance: _LifecycleAdvance, now: datetime
    ) -> bool:
        """Apply one fold advance through the lifecycle CAS.

        Carries the command's own session/sequence forward (the outbox
        TTL expiry convention) with the cycle's bus time. A lost CAS
        means another writer moved the row concurrently — the next
        cycle re-reads and re-folds, so losing is a debug-level no-op.

        Args:
            cmd: Active command row being advanced.
            advance: Fold target computed by ``_fold_lifecycle_advance``.
            now: Cycle bus time.

        Returns:
            True when the CAS applied.
        """
        applied = await self._repo.advance_trade_command_lifecycle(
            public_id=cmd["public_id"],
            expected_status=cmd["status"],
            new_status=advance.status,
            bus_time=now,
            session_id=cmd["session_id"],
            sequence_id=cmd["sequence_id"],
            acked_at=advance.acked_at,
            exchange_order_id=advance.exchange_order_id,
            terminal_at=advance.terminal_at,
            last_error=advance.last_error,
        )
        if applied:
            logger.info(
                f"ReconciliationLoop[{self._exchange}] advanced command "
                f"{cmd['public_id']} {cmd['status']} -> {advance.status} "
                f"(cid={cmd['client_order_id']})"
            )
        else:
            logger.debug(
                f"ReconciliationLoop[{self._exchange}] lost lifecycle CAS for "
                f"{cmd['public_id']} (status moved concurrently); refolding next cycle"
            )
        return applied

    def _filter_owned_commands(self, commands: list[TradeCommandRow]) -> list[TradeCommandRow]:
        """Filter command rows to shards owned by this loop instance."""
        if self._ownership is None:
            return commands
        return [cmd for cmd in commands if self._ownership.owns(cmd["shard_key"])]

    def _owns_shard(self, shard_key: str) -> bool:
        """Return whether this loop instance owns a shard key."""
        return self._ownership is None or self._ownership.owns(shard_key)

    def _is_exchange_shard(self, shard_key: str) -> bool:
        """Return whether a shard key belongs to this loop's exchange."""
        parsed = parse_shard_key(shard_key)
        return parsed is not None and parsed[0] == self._exchange

    async def _load_active_commands(self, now: datetime) -> list[TradeCommandRow]:
        """Load active exchange commands visible to this loop instance."""
        commands = await self._repo.get_active_commands_for_exchange(
            exchange=self._exchange, as_of=now
        )
        return self._filter_owned_commands(commands)

    @staticmethod
    def _foldable_commands(commands: list[TradeCommandRow]) -> list[TradeCommandRow]:
        """Return active commands whose lifecycle is folded from venue events."""
        return [cmd for cmd in commands if cmd["command_type"] in _FOLD_COMMAND_TYPES]

    @staticmethod
    def _group_events_by_cid(events: list[VenueEventRow]) -> dict[str, list[VenueEventRow]]:
        """Group lifecycle events by non-empty client order id."""
        events_by_cid: dict[str, list[VenueEventRow]] = {}
        for event in events:
            cid = event["client_order_id"]
            if cid:
                events_by_cid.setdefault(cid, []).append(event)
        return events_by_cid

    async def _load_events_by_cid(
        self, fold_commands: list[TradeCommandRow]
    ) -> dict[str, list[VenueEventRow]]:
        """Load and group lifecycle events for foldable commands."""
        if not fold_commands:
            return {}
        events = await self._repo.get_order_lifecycle_events(
            [cmd["client_order_id"] for cmd in fold_commands]
        )
        return self._group_events_by_cid(events)

    @staticmethod
    def _command_events(
        cmd: TradeCommandRow, events_by_cid: dict[str, list[VenueEventRow]]
    ) -> list[VenueEventRow]:
        """Return fold-visible events for a command."""
        if cmd["command_type"] not in _FOLD_COMMAND_TYPES:
            return []
        return events_by_cid.get(cmd["client_order_id"], [])

    @staticmethod
    def _command_advance(
        cmd: TradeCommandRow, events_by_cid: dict[str, list[VenueEventRow]]
    ) -> _LifecycleAdvance | None:
        """Return a lifecycle advance for a foldable command."""
        if cmd["status"] not in _FOLD_SOURCE_STATUSES:
            return None
        return _fold_lifecycle_advance(cmd, events_by_cid.get(cmd["client_order_id"], []))

    async def _advance_foldable_commands(
        self,
        fold_commands: list[TradeCommandRow],
        events_by_cid: dict[str, list[VenueEventRow]],
        now: datetime,
    ) -> int:
        """Apply lifecycle advances for foldable commands."""
        advanced = 0
        for cmd in fold_commands:
            advance = self._command_advance(cmd, events_by_cid)
            if advance is not None and await self._advance_command(cmd, advance, now):
                advanced += 1
        return advanced

    async def _resurrect_falsely_rejected(self, now: datetime) -> int:
        """Restore REJECTED commands whose cid shows later live evidence.

        Restart-proof backstop for the false-absence-rejection heal: the
        executor's in-memory restore queue dies with its process, but a
        durably REJECTED row with an ``order_accepted``/``fill_observed``
        event LATER than its rejection is proof the rejection was wrong
        (or legally retried) — terminal rows are invisible to the normal
        fold (the active-command read excludes them), so without this
        pass they would stay wrong forever and the paired-leg backstop
        would keep projecting a false terminal. Resurrection CAS-es the
        row to ACCEPTED with the stale terminal stamp cleared; the NEXT
        cycle's fold re-derives the true state from the full event
        history. Bounded per cycle; CAS misses skip.

        Args:
            now: Cycle bus time.

        Returns:
            Number of rows resurrected.
        """
        rows = await self._repo.get_rejected_commands_with_later_live_evidence(self._exchange)
        if self._ownership is not None:
            rows = [cmd for cmd in rows if self._ownership.owns(cmd["shard_key"])]
        resurrected = 0
        for cmd in rows:
            applied = await self._repo.advance_trade_command_lifecycle(
                public_id=cmd["public_id"],
                expected_status=TradeCommandStatusEnum.REJECTED,
                new_status=TradeCommandStatusEnum.ACCEPTED,
                bus_time=now,
                session_id=cmd["session_id"],
                sequence_id=cmd["sequence_id"],
                acked_at=now,
                last_error="resurrected: live venue evidence postdates the rejection",
                clear_terminal_at=True,
            )
            if applied:
                resurrected += 1
                logger.warning(
                    f"ReconciliationLoop[{self._exchange}] RESURRECTED command "
                    f"{cmd['public_id']} (cid={cmd['client_order_id']}) — rejected "
                    f"durably but live venue evidence postdates the rejection"
                )
        return resurrected

    def _is_stale(self, cmd: TradeCommandRow, now: datetime) -> bool:
        """Return whether a command is old enough for stale reporting."""
        age = (now - cmd["created_at"]).total_seconds()
        return age > self._interval * 3

    def _report_stale(self, cmd: TradeCommandRow, age: float, events: list[VenueEventRow]) -> bool:
        """Report one over-age command according to its evidence class.

        Zero evidence WARNs (true anomaly: dispatched but the venue
        plane never heard of it — the executor verification sweep's
        work queue). Unknown-only evidence is INFO (the sweep is
        already verifying it). Real evidence is silent: the fold owns
        those rows and an old open order is healthy. The caller passes
        an EMPTY event list for non-fold command types (a cancel shares
        the original order's cid, and the create's evidence must not
        silence a stuck cancel), so those keep the legacy WARN.

        Args:
            cmd: The over-age command row.
            age: Command age in seconds.
            events: The command's lifecycle events (possibly empty).

        Returns:
            True when the command was reported as stale.
        """
        if not events:
            logger.warning(
                f"ReconciliationLoop[{self._exchange}] stale command "
                f"{cmd['public_id']} status={cmd['status']} age={age:.0f}s "
                f"shard={cmd['shard_key']} — no venue evidence"
            )
            return True
        if all(event["event_type"] == "order_submit_unknown" for event in events):
            logger.info(
                f"ReconciliationLoop[{self._exchange}] command {cmd['public_id']} "
                f"age={age:.0f}s remains UNKNOWN (executor verification pending)"
            )
            return True
        return False

    def _collect_stale_report(
        self,
        commands: list[TradeCommandRow],
        events_by_cid: dict[str, list[VenueEventRow]],
        now: datetime,
    ) -> tuple[int, set[str]]:
        """Report stale commands and return count plus observed shards."""
        stale_count = 0
        seen_shards: set[str] = set()
        for cmd in commands:
            seen_shards.add(cmd["shard_key"])
            if not self._is_stale(cmd, now):
                continue
            age = (now - cmd["created_at"]).total_seconds()
            if self._report_stale(cmd, age, self._command_events(cmd, events_by_cid)):
                stale_count += 1
        return stale_count, seen_shards

    def _log_cycle_result(self, advanced: int, stale_count: int) -> None:
        """Emit reconciliation cycle summary logs."""
        if advanced > 0:
            logger.info(
                f"ReconciliationLoop[{self._exchange}] folded {advanced} command "
                f"lifecycle advances from venue evidence"
            )
        if stale_count > 0:
            logger.info(f"ReconciliationLoop[{self._exchange}] found {stale_count} stale commands")

    def _record_cycle_successes(self, seen_shards: set[str]) -> None:
        """Record reconciliation success for every shard seen this cycle."""
        self._last_seen_shards.update(seen_shards)
        for shard_key in seen_shards:
            self._trade_service.record_recon_success(shard_key)

    def _failure_shards(self) -> set[str]:
        """Return real shard keys that should receive a cycle failure."""
        candidates = self._last_seen_shards | self._trade_service.known_shard_keys()
        return {
            shard_key
            for shard_key in candidates
            if self._is_exchange_shard(shard_key) and self._owns_shard(shard_key)
        }

    def _record_cycle_failure(self) -> None:
        """Record a reconciliation failure and log shard halt transitions."""
        logger.exception(f"ReconciliationLoop[{self._exchange}] cycle failed")
        failure_shards = self._failure_shards()
        if not failure_shards:
            logger.error(
                f"ReconciliationLoop[{self._exchange}] cycle failed with no known real shard "
                f"to mark"
            )
            return
        for shard_key in sorted(failure_shards):
            halted = self._trade_service.record_recon_failure(shard_key)
            if halted:
                logger.error(
                    f"ReconciliationLoop[{self._exchange}] shard {shard_key} HALTED "
                    f"due to consecutive reconciliation failures"
                )

    async def _reconcile_cycle(self) -> None:
        """Execute one reconciliation cycle.

        Queries active commands, folds venue evidence into durable
        status advances, reports genuinely stale rows, and records
        success/failure for the circuit breaker.
        """
        now = datetime.now(UTC)
        try:
            active_cmds = await self._load_active_commands(now)
            fold_cmds = self._foldable_commands(active_cmds)
            events_by_cid = await self._load_events_by_cid(fold_cmds)
            advanced = await self._advance_foldable_commands(fold_cmds, events_by_cid, now)
            advanced += await self._resurrect_falsely_rejected(now)
            stale_count, seen_shards = self._collect_stale_report(active_cmds, events_by_cid, now)
            self._log_cycle_result(advanced, stale_count)
            self._record_cycle_successes(seen_shards)
            logger.debug(f"ReconciliationLoop[{self._exchange}] cycle completed OK")
        except Exception:
            self._record_cycle_failure()

    def stop(self) -> None:
        """Signal the reconciliation loop to stop."""
        self._running = False
