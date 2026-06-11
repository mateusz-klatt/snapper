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
- A scan exception records a reconciliation failure on the exchange
  fallback shard; repeated failures halt that shard via
  :class:`TradeService`.
"""

import asyncio
from dataclasses import dataclass
from datetime import UTC
from datetime import datetime

from loguru import logger

from snapper.application.trade.trade_service import TradeService
from snapper.core.partitioning import ShardOwnership
from snapper.core.types import ExchangeEnum
from snapper.core.types import ExecutionModeEnum
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
    acked_at: datetime | None = None
    exchange_order_id: str | None = None
    max_cum: float | None = None
    fill_complete = False
    terminal_status: TradeCommandStatusEnum | None = None
    terminal_at: datetime | None = None
    last_error: str | None = None

    def _supersede_rejection() -> None:
        """Clear an earlier REJECTED target once later live evidence lands."""
        nonlocal terminal_status, terminal_at, last_error
        if terminal_status is TradeCommandStatusEnum.REJECTED:
            terminal_status = None
            terminal_at = None
            last_error = None

    for event in events:
        event_type = event["event_type"]
        if event_type == "order_accepted":
            _supersede_rejection()
            if acked_at is None:
                acked_at = event["received_at"]
            if event["exchange_order_id"]:
                exchange_order_id = event["exchange_order_id"]
        elif event_type == "fill_observed":
            _supersede_rejection()
            cum = event["cum_fill_size"]
            if cum is not None and (max_cum is None or cum > max_cum):
                max_cum = cum
            if event["status"] == "filled":
                fill_complete = True
            if exchange_order_id is None and event["exchange_order_id"]:
                exchange_order_id = event["exchange_order_id"]
        elif event_type == "order_rejected":
            terminal_status = TradeCommandStatusEnum.REJECTED
            terminal_at = event["received_at"]
            last_error = event["error"] or "rejected by venue"
        elif event_type == "order_breaker_open":
            terminal_status = TradeCommandStatusEnum.FAILED
            terminal_at = event["received_at"]
            last_error = "circuit_breaker_open"
        elif event_type == "order_terminal":
            raw_status = (event["status"] or "").lower()
            mapped = _TERMINAL_EVENT_STATUS_MAP.get(raw_status)
            terminal_status = mapped or TradeCommandStatusEnum.CANCELLED
            terminal_at = event["received_at"]
            last_error = None if mapped else f"unmapped terminal status {raw_status!r}"
    if terminal_status is not None:
        target = terminal_status
    elif max_cum is not None and max_cum > 0:
        quantity = cmd["quantity"]
        filled_completely = fill_complete or (
            bool(quantity) and max_cum >= quantity * (1 - _FILL_COMPLETE_REL_TOL)
        )
        target = (
            TradeCommandStatusEnum.FILLED
            if filled_completely
            else TradeCommandStatusEnum.PARTIALLY_FILLED
        )
        terminal_at = None
        last_error = None
    elif acked_at is not None:
        target = TradeCommandStatusEnum.ACCEPTED
        terminal_at = None
        last_error = None
    else:
        return None
    if _STATUS_RANK[target] <= _STATUS_RANK.get(cmd["status"], 0):
        return None
    return _LifecycleAdvance(
        status=target,
        acked_at=acked_at,
        exchange_order_id=exchange_order_id,
        terminal_at=terminal_at,
        last_error=last_error,
    )


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

    async def _reconcile_cycle(self) -> None:
        """Execute one reconciliation cycle.

        Queries active commands, folds venue evidence into durable
        status advances, reports genuinely stale rows, and records
        success/failure for the circuit breaker.
        """
        now = datetime.now(UTC)
        try:
            active_cmds = await self._repo.get_active_commands_for_exchange(
                exchange=self._exchange, as_of=now
            )
            if self._ownership is not None:
                active_cmds = [cmd for cmd in active_cmds if self._ownership.owns(cmd["shard_key"])]
            fold_cmds = [cmd for cmd in active_cmds if cmd["command_type"] in _FOLD_COMMAND_TYPES]
            events_by_cid: dict[str, list[VenueEventRow]] = {}
            if fold_cmds:
                events = await self._repo.get_order_lifecycle_events(
                    [cmd["client_order_id"] for cmd in fold_cmds]
                )
                for event in events:
                    cid = event["client_order_id"]
                    if cid:
                        events_by_cid.setdefault(cid, []).append(event)
            advanced = 0
            for cmd in fold_cmds:
                if cmd["status"] not in _FOLD_SOURCE_STATUSES:
                    continue
                advance = _fold_lifecycle_advance(
                    cmd, events_by_cid.get(cmd["client_order_id"], [])
                )
                if advance is None:
                    continue
                if await self._advance_command(cmd, advance, now):
                    advanced += 1
            stale_count = 0
            seen_shards: set[str] = set()
            for cmd in active_cmds:
                seen_shards.add(cmd["shard_key"])
                age = (now - cmd["created_at"]).total_seconds()
                if age <= self._interval * 3:
                    continue
                cmd_events = (
                    events_by_cid.get(cmd["client_order_id"], [])
                    if cmd["command_type"] in _FOLD_COMMAND_TYPES
                    else []
                )
                if self._report_stale(cmd, age, cmd_events):
                    stale_count += 1

            if advanced > 0:
                logger.info(
                    f"ReconciliationLoop[{self._exchange}] folded {advanced} command "
                    f"lifecycle advances from venue evidence"
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
