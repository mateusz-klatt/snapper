"""Trade domain service for in-memory trade runtime projections.

TradeService consumes VenueEvent-shaped rows and updates per-shard
command state, position state, cash and turnover state, execution
deduplication, and reconciliation circuit-breaker counters. It exposes
the live read model used inside the trade runtime and produces snapshots
that TraderCoordinator persists as TradeProjectionCheckpoint rows.
Canonical Order and Execution rows are persisted on the
executor / exchange-client path.
"""

import json
import math
from collections import OrderedDict
from dataclasses import dataclass
from dataclasses import field
from datetime import UTC
from datetime import datetime
from typing import Final
from typing import Literal
from typing import TypedDict

from loguru import logger

from snapper.core.types import FillStatusEnum
from snapper.core.types import TradeCommandStatusEnum
from snapper.core.types import TradeSideEnum
from snapper.data.repository_types import AccrualLedgerRow
from snapper.data.repository_types import TradeCommandRow
from snapper.data.repository_types import VenueEventRow

TERMINAL_STATUSES: Final[frozenset[str]] = frozenset(
    {
        TradeCommandStatusEnum.FILLED,
        TradeCommandStatusEnum.CANCELLED,
        TradeCommandStatusEnum.EXPIRED,
        TradeCommandStatusEnum.REJECTED,
        TradeCommandStatusEnum.FAILED,
    }
)

FILL_EVENT_TYPES: Final[frozenset[str]] = frozenset({"fill_observed"})


@dataclass
class PositionProjection:
    """In-memory projection of position state for a single shard.

    Updated on every confirmed fill. Read by TradingEngineService for
    sizing, risk, and stop-loss decisions.

    ``position_opened_at`` records the venue timestamp at which the
    current open cycle was opened (zero-crossing on the long or short
    side). It is reset to ``None`` whenever the position returns to
    flat. The funding accrual subsystem (the funding fee model)
    uses it to clamp catch-up boundaries to the current open cycle so
    accruals from a previous cycle do not retro-charge a freshly
    reopened position.
    """

    position_qty: float = 0.0
    entry_price: float | None = None
    realized_pnl: float = 0.0
    position_opened_at: datetime | None = None


@dataclass
class CommandState:
    """In-memory state of the active (non-terminal) trade command for a shard.

    Read by TradingEngineService to determine if an order is in-flight.
    """

    command_public_id: str | None = None
    status: str | None = None
    client_order_id: str | None = None
    exchange_order_id: str | None = None
    in_flight: bool = False


@dataclass
class ShardState:
    """Aggregate in-memory state for a single shard_key.

    ``active_cycle_public_id`` and ``active_cycle_max_qty`` cache the
    public_id and peak absolute quantity of the current open
    ``position_cycles`` row for this shard. The trader (not TradeService)
    hydrates these fields at fill-sync time and at startup reconciliation
    so the hot path can issue close/flip/update_max_qty calls without
    an extra DB lookup. The cache is authoritative while live; on
    process restart the trader re-reads DB to rebuild it, because
    checkpoint persistence does not cover these two fields.
    """

    position: PositionProjection = field(default_factory=PositionProjection)
    command: CommandState = field(default_factory=CommandState)
    cash: float = 10_000.0
    peak_equity: float = 10_000.0
    turnover: float = 0.0
    last_venue_event_id: int = 0
    seen_exec_ids: OrderedDict[str, None] = field(default_factory=OrderedDict)
    halted: bool = False
    halt_reasons: set[str] = field(default_factory=set)
    recon_failure_count: int = 0
    active_cycle_public_id: str | None = None
    active_cycle_max_qty: float = 0.0


class FillProjection(TypedDict):
    """Fill-derived shard state produced by a chronological venue-event replay.

    Carries exactly the fields a dropped fill corrupts and a from-scratch
    venue-event replay reconstructs. R9 gap recovery overlays these onto a
    live shard without disturbing command identity or ``peak_equity`` (which
    venue events cannot carry and the checkpoint restore owns).
    """

    position_qty: float
    entry_price: float | None
    position_opened_at: datetime | None
    realized_pnl: float
    cash: float
    turnover: float
    seen_exec_ids: OrderedDict[str, None]
    last_venue_event_id: int


class TradeService:
    """In-memory trade runtime projection service.

    Maintains per-shard order lifecycle state, position state, recovery
    watermarks, and circuit-breaker counters. Exposes read-model accessors
    for the runtime and produces checkpoint snapshots consumed by
    TraderCoordinator.
    """

    def __init__(self, initial_cash: float = 10_000.0) -> None:
        """Initialize trade service.

        Args:
            initial_cash: Default cash for new shards.
        """
        self._shards: dict[str, ShardState] = {}
        self._initial_cash = initial_cash

    def _get_or_create_shard(self, shard_key: str) -> ShardState:
        """Get existing shard state or create a new one."""
        if shard_key not in self._shards:
            self._shards[shard_key] = ShardState(
                cash=self._initial_cash,
                peak_equity=self._initial_cash,
            )
        return self._shards[shard_key]

    def known_shard_keys(self) -> set[str]:
        """Return shard keys that already have in-memory state.

        Returns:
            Snapshot of materialized trading shard keys.
        """
        return set(self._shards)

    def get_position(self, shard_key: str) -> PositionProjection:
        """Read model: current position for engine sizing/risk decisions.

        Args:
            shard_key: Unique identifier for the trading shard.

        Returns:
            PositionProjection with current quantity, entry price, and
            realized PnL.
        """
        return self._get_or_create_shard(shard_key).position

    def add_funding_accrual(self, shard_key: str, amount: float) -> None:
        """Apply a funding/rollover charge to shard state.

        Deducts ``amount`` from cash and realized PnL. Positive values
        represent charges (reduce cash), negative values represent
        credits (increase cash). The caller is responsible for
        persisting the ``AccrualLedger`` row before calling this method
        so that crash recovery can replay from the ledger.

        Args:
            shard_key: Unique identifier for the trading shard.
            amount: Signed charge in the notional asset. Positive
                means the position holder pays, negative means the
                holder receives.
        """
        shard = self._get_or_create_shard(shard_key)
        shard.cash -= amount
        shard.position.realized_pnl -= amount

    def replay_funding_accruals(self, shard_key: str, accruals: list[AccrualLedgerRow]) -> None:
        """Replay persisted accrual rows into shard state for crash recovery.

        Called during ``_recover_from_checkpoints`` to re-apply accrual
        charges that were committed to the ledger but not yet captured
        in the most recent checkpoint snapshot.

        Args:
            shard_key: Unique identifier for the trading shard.
            accruals: Ordered list of accrual rows from the recovery
                window (checkpoint_at, now].
        """
        for row in accruals:
            self.add_funding_accrual(shard_key, row["amount"])

    def get_command_state(self, shard_key: str) -> CommandState:
        """Read model: current command state for engine in-flight guard.

        Args:
            shard_key: Unique identifier for the trading shard.

        Returns:
            CommandState with current order status and in-flight flag.
        """
        return self._get_or_create_shard(shard_key).command

    def get_equity(self, shard_key: str) -> float:
        """Read model: current cash (equity without mark-to-market).

        Args:
            shard_key: Unique identifier for the trading shard.

        Returns:
            Current cash balance for the shard.
        """
        return self._get_or_create_shard(shard_key).cash

    def get_peak_equity(self, shard_key: str) -> float:
        """Read model: peak equity for drawdown calculation.

        Args:
            shard_key: Unique identifier for the trading shard.

        Returns:
            Historical peak equity watermark for the shard.
        """
        return self._get_or_create_shard(shard_key).peak_equity

    def is_halted(self, shard_key: str) -> bool:
        """Read model: whether the shard is halted by circuit breaker.

        Args:
            shard_key: Unique identifier for the trading shard.

        Returns:
            True if the shard is currently halted.
        """
        return self._get_or_create_shard(shard_key).halted

    def register_command(self, shard_key: str, cmd: TradeCommandRow) -> None:
        """Register a newly created trade command in the in-memory state.

        Use when a caller has already inserted a ``TradeCommand`` row
        and needs the runtime projection to reflect the command as
        in-flight.

        Args:
            shard_key: Unique identifier for the trading shard.
            cmd: Trade command row containing public_id, status, and
                client_order_id fields.
        """
        shard = self._get_or_create_shard(shard_key)
        shard.command = CommandState(
            command_public_id=cmd["public_id"],
            status=cmd["status"],
            client_order_id=cmd["client_order_id"],
            in_flight=True,
        )

    def apply_venue_event(self, event: VenueEventRow) -> None:
        """Apply a venue event to the in-memory projection.

        Handles all event types: order_accepted, order_rejected,
        fill_observed, order_terminal, order_submit_unknown and
        order_breaker_open (a rejection-equivalent terminal — the live
        REJECTED publish released engine intent, so replay and
        checkpoint recovery must converge to the same terminal state).
        Updates position, command state, cash, and watermark.

        Args:
            event: Venue event row to apply. Must contain shard_key,
                event_type, and id fields at minimum.
        """
        shard = self._get_or_create_shard(event["shard_key"])
        self._apply_event_to_shard(shard, event)

    def _apply_event_to_shard(self, shard: ShardState, event: VenueEventRow) -> None:
        """Dispatch one venue event onto a specific shard, enforcing id ordering.

        Shared by :meth:`apply_venue_event` (live/delta replay onto the stored
        shard) and :meth:`project_fill_state_from_events` (replay onto a
        throwaway shard), so both honour the monotonic-id guard and identical
        per-type handling.
        """
        event_type = event["event_type"]
        event_id = event["id"]

        if event_id <= shard.last_venue_event_id:
            return

        if event_type == "order_accepted":
            self._apply_order_accepted(shard, event)
        elif event_type in ("order_rejected", "order_breaker_open"):
            self._apply_order_terminal(shard, event)
        elif event_type == "fill_observed":
            self._apply_fill(shard, event)
        elif event_type == "order_terminal":
            self._apply_order_terminal(shard, event)
        elif event_type == "order_submit_unknown":
            self._apply_order_submit_unknown(event)
        else:
            logger.warning(f"TradeService: unknown venue event type: {event_type}")

        shard.last_venue_event_id = event_id

    def reset_shard(self, shard_key: str) -> None:
        """Replace a shard's in-memory state with a fresh projection.

        Used by R9 venue-plane gap recovery before a from-scratch chronological
        replay, so the rebuild starts from the canonical ``self._initial_cash``
        with no carried-over position, cash, dedup, or watermark state.

        Args:
            shard_key: Shard to reset.
        """
        self._shards[shard_key] = ShardState(
            cash=self._initial_cash,
            peak_equity=self._initial_cash,
        )

    @staticmethod
    def dedup_fill_events(events: list[VenueEventRow]) -> list[VenueEventRow]:
        """Drop redelivered duplicate fill_observed rows by identity, order-preserving.

        A from-scratch venue replay can span a shard's whole history. The live
        ``_dedup_fill`` set is bounded (10k, FIFO-evicted), so two duplicate
        fill rows (the same fill redelivered after a publish retry) separated by
        more than that window would each apply and double-book. This collapses
        later duplicates UP FRONT with an unbounded set — mirroring
        ``_dedup_fill``'s either-key identity (exec_id OR trade_id, else the
        client_order_id+size+price fallback) — so a single-pass replay is exact
        regardless of history length. Non-fill lifecycle events pass through.

        Args:
            events: Id-ordered venue events to replay.

        Returns:
            The events with duplicate fill identities removed (first kept).
        """
        seen: set[str] = set()
        result: list[VenueEventRow] = []
        for event in events:
            if event["event_type"] != "fill_observed":
                result.append(event)
                continue
            exec_id = event.get("exec_id")
            trade_id = event.get("trade_id")
            keys = [key for key in (exec_id, trade_id) if key]
            if not keys:
                keys = [
                    f"fallback-{event.get('client_order_id')}"
                    f"-{event.get('fill_size')}-{event.get('fill_price')}"
                ]
            if any(key in seen for key in keys):
                continue
            seen.update(keys)
            result.append(event)
        return result

    def project_fill_state_from_events(self, events: list[VenueEventRow]) -> FillProjection:
        """Replay events into a THROWAWAY shard and return fill-derived state.

        Applies the full id-ordered venue-event history to a fresh
        ``ShardState`` that is NOT stored in ``self._shards`` (no global
        mutation), so checkpoint-path gap recovery can overlay only the
        fill-derived fields onto the live shard without disturbing the command
        identity and ``peak_equity`` the checkpoint restore established. Cash
        reconstructs exactly as the live path builds it
        (``self._initial_cash`` + fill flows).

        Args:
            events: Full id-ordered ``fill_observed``/lifecycle venue events.

        Returns:
            The fill-derived projection for overlay.
        """
        temp = ShardState(cash=self._initial_cash, peak_equity=self._initial_cash)
        for event in self.dedup_fill_events(events):
            self._apply_event_to_shard(temp, event)
        return {
            "position_qty": temp.position.position_qty,
            "entry_price": temp.position.entry_price,
            "position_opened_at": temp.position.position_opened_at,
            "realized_pnl": temp.position.realized_pnl,
            "cash": temp.cash,
            "turnover": temp.turnover,
            "seen_exec_ids": temp.seen_exec_ids,
            "last_venue_event_id": temp.last_venue_event_id,
        }

    def overlay_fill_state(self, shard_key: str, projection: FillProjection) -> None:
        """Overlay gap-corrected fill-derived fields onto a live shard.

        Copies ONLY the fill-derived projection (position, entry, opened-at,
        realized PnL, cash, turnover, dedup set, watermark) produced by a
        chronological venue-event replay, leaving command identity and
        ``peak_equity`` exactly as the checkpoint restore left them. Used by
        checkpoint-path R9 recovery to correct a fill dropped under the scalar
        watermark without changing non-fill recovery semantics.

        Args:
            shard_key: Shard to correct.
            projection: Fill-derived state from
                :meth:`project_fill_state_from_events`.
        """
        shard = self._get_or_create_shard(shard_key)
        shard.position.position_qty = projection["position_qty"]
        shard.position.entry_price = projection["entry_price"]
        shard.position.position_opened_at = projection["position_opened_at"]
        shard.position.realized_pnl = projection["realized_pnl"]
        shard.cash = projection["cash"]
        shard.turnover = projection["turnover"]
        shard.seen_exec_ids = projection["seen_exec_ids"]
        shard.last_venue_event_id = projection["last_venue_event_id"]

    def _apply_order_accepted(self, shard: ShardState, event: VenueEventRow) -> None:
        """Update command state on venue acceptance."""
        shard.command.status = TradeCommandStatusEnum.ACCEPTED
        shard.command.exchange_order_id = event.get("exchange_order_id")

    def _apply_order_submit_unknown(self, event: VenueEventRow) -> None:
        """Hold command state on an ambiguous submit outcome.

        The submit failed in a way where the order MAY exist on the
        venue. Deliberately changes nothing: the command
        stays non-terminal (in_flight remains True), no rejection is
        recorded, and only the watermark advances — so a checkpoint
        replay reproduces the held state instead of warning about an
        unrecognized event type. Resolution arrives later as a regular
        order_accepted or order_rejected event from the executor's
        venue verification.
        """
        logger.warning(
            f"TradeService: order submit UNKNOWN for "
            f"{event.get('client_order_id')} on {event['shard_key']} — "
            f"holding command state until venue verification resolves"
        )

    def _apply_fill(self, shard: ShardState, event: VenueEventRow) -> None:
        """Apply a fill to position and cash projection."""
        if not self._dedup_fill(shard, event):
            return

        fill_size = event.get("fill_size") or 0.0
        fill_price = event.get("fill_price") or 0.0
        side = event.get("side") or ""
        fee = event.get("fee") or 0.0
        notional = fill_size * fill_price
        side_lower = side.lower()
        event_time = event.get("venue_timestamp") or event["received_at"]

        if side_lower in (TradeSideEnum.BUY, TradeSideEnum.SELL):
            signed_qty = fill_size if side_lower == TradeSideEnum.BUY else -fill_size
            self._update_position(shard.position, signed_qty, fill_size, fill_price, event_time)
            self._update_cash(shard, side_lower, notional, fee)

        shard.turnover += notional
        self._update_command_fill_status(shard, event)

    def _dedup_fill(self, shard: ShardState, event: VenueEventRow) -> bool:
        """Return True if the fill is new and should be applied.

        A fill is a duplicate if EITHER its ``exec_id`` OR its ``trade_id``
        was already seen, not merely the preferred key. The execution plane
        (full replay) carries only ``trade_id`` (its ``exec_id`` is ``None``)
        while the venue-events plane (delta replay) carries both, so probing
        only the preferred key let the SAME fill re-apply across planes once a
        conservative recovery watermark replayed it again. Both keys are
        recorded on apply and both are probed on dedup so cross-plane
        re-replay is idempotent regardless of which plane recorded it first.

        The id-less fallback key mirrors the live engine's apply_fill
        fallback shape (client_order_id + fill size + fill price) instead
        of the venue_events row PK: id-less fills (Walutomat cumulative
        polls carry no venue exec id) must dedupe ACROSS planes — a
        recovery republish seen live by the engine and the same row seen
        by checkpoint replay have no common row id, so a row-PK fallback
        let the two planes double-apply the same quantity after a
        coordinator restart.
        """
        exec_id = event.get("exec_id")
        trade_id = event.get("trade_id")
        if exec_id and exec_id in shard.seen_exec_ids:
            return False
        if trade_id and trade_id in shard.seen_exec_ids:
            return False
        if not exec_id and not trade_id:
            fallback = (
                f"fallback-{event.get('client_order_id')}"
                f"-{event.get('fill_size')}-{event.get('fill_price')}"
            )
            if fallback in shard.seen_exec_ids:
                return False
            shard.seen_exec_ids[fallback] = None
        else:
            if exec_id:
                shard.seen_exec_ids[exec_id] = None
            if trade_id:
                shard.seen_exec_ids[trade_id] = None
        while len(shard.seen_exec_ids) > 10_000:
            shard.seen_exec_ids.popitem(last=False)
        return True

    @staticmethod
    def _detect_cycle_transition(
        old_qty: float, new_qty: float
    ) -> Literal["open", "close", "flip", "scale_up"] | None:
        """Classify a shard position change into a cycle lifecycle transition.

        Pure helper with no side effects — the trader uses the result to
        decide which ``position_cycles`` repository call to issue on a
        given fill, keeping TradeService free of any DB awareness.

        ``open``: a flat position became non-flat (``|old| < eps`` and
        ``|new| >= eps``). A new cycle row must be inserted.

        ``close``: a non-flat position returned to zero (``|old| >= eps``
        and ``|new| < eps``). The active cycle must be SCD2-closed.

        ``flip``: direction reversed in a single fill — both sides are
        non-flat but with opposite signs. The existing cycle is closed
        and a new one is opened in the same transaction.

        ``scale_up``: direction was preserved (neither end was flat, same
        sign) and the new absolute quantity strictly exceeds the old
        one. Only the peak needs to move; the cycle row stays the same.

        ``None``: all other cases — flat-to-flat, scale-down / hold
        where ``|new| <= |old|``, or any shape that does not require a
        DB write. The trader should treat this as a no-op.

        The epsilon is ``1e-12`` to match the zero-snap used by
        :meth:`_update_position` — quantities below this threshold are
        indistinguishable from flat in the existing position math.

        Args:
            old_qty: Signed position quantity before the fill.
            new_qty: Signed position quantity after the fill.

        Returns:
            One of ``"open"``, ``"close"``, ``"flip"``, ``"scale_up"``,
            or ``None``.
        """
        eps = 1e-12
        old_flat = abs(old_qty) < eps
        new_flat = abs(new_qty) < eps
        if old_flat and new_flat:
            return None
        if old_flat:
            return "open"
        if new_flat:
            return "close"
        same_sign = (old_qty > 0.0) == (new_qty > 0.0)
        if not same_sign:
            return "flip"
        if abs(new_qty) > abs(old_qty):
            return "scale_up"
        return None

    def _update_position(
        self,
        pos: PositionProjection,
        signed_qty: float,
        fill_size: float,
        fill_price: float,
        event_time: datetime,
    ) -> None:
        """Update position quantity and entry price for a fill.

        Args:
            pos: Position projection to mutate in place.
            signed_qty: Fill quantity with sign (positive for BUY,
                negative for SELL).
            fill_size: Unsigned fill quantity.
            fill_price: Fill execution price.
            event_time: Venue (or fallback bus) timestamp at which the
                fill occurred. Stamped onto ``position_opened_at`` at
                every zero-crossing so the funding accrual loop can
                clamp catch-up boundaries to the current open cycle.
        """
        is_increasing = (pos.position_qty >= 0 and signed_qty > 0) or (
            pos.position_qty <= 0 and signed_qty < 0
        )
        if is_increasing:
            self._increase_position(pos, fill_size, fill_price, event_time)
        else:
            self._decrease_position(pos, fill_size, fill_price, event_time)

        pos.position_qty += signed_qty
        if abs(pos.position_qty) < 1e-12:
            pos.position_qty = 0.0
            pos.entry_price = None
            pos.position_opened_at = None

    @staticmethod
    def _increase_position(
        pos: PositionProjection,
        fill_size: float,
        fill_price: float,
        event_time: datetime,
    ) -> None:
        """Recalculate weighted-average entry price for a position-increasing fill.

        When the helper is called from a flat position (``entry_price``
        is None), it stamps ``position_opened_at`` with ``event_time``.
        VWAP-only updates (adding to an existing same-direction
        position) preserve the original ``position_opened_at``.
        """
        old_qty = abs(pos.position_qty)
        new_qty = old_qty + fill_size
        if pos.entry_price is not None and old_qty > 0 and new_qty > 0:
            pos.entry_price = (old_qty * pos.entry_price + fill_size * fill_price) / new_qty
        else:
            pos.entry_price = fill_price
            pos.position_opened_at = event_time

    @staticmethod
    def _decrease_position(
        pos: PositionProjection,
        fill_size: float,
        fill_price: float,
        event_time: datetime,
    ) -> None:
        """Realize PnL and handle overshoot for a position-decreasing fill.

        On overshoot (flip transition: long-to-short or short-to-long),
        the helper resets ``position_opened_at`` to ``event_time``
        because the new opposite-side position opens at the fill.
        """
        close_qty = min(fill_size, abs(pos.position_qty))
        overshoot = fill_size - close_qty
        if pos.entry_price is not None and close_qty > 0:
            pnl_per_unit = fill_price - pos.entry_price
            if pos.position_qty < 0:
                pnl_per_unit = pos.entry_price - fill_price
            pos.realized_pnl += close_qty * pnl_per_unit
        if overshoot > 1e-12:
            pos.entry_price = fill_price
            pos.position_opened_at = event_time

    @staticmethod
    def _update_cash(shard: ShardState, side: str, notional: float, fee: float) -> None:
        """Adjust cash balance for a buy or sell fill."""
        if side == TradeSideEnum.BUY:
            shard.cash -= notional + fee
        else:
            shard.cash += notional - fee

    def _update_command_fill_status(self, shard: ShardState, event: VenueEventRow) -> None:
        """Update command FSM based on fill status field."""
        status = event.get("status")
        if status == FillStatusEnum.FILLED:
            shard.command.status = TradeCommandStatusEnum.FILLED
            shard.command.in_flight = False
        elif status == FillStatusEnum.PARTIAL:
            shard.command.status = TradeCommandStatusEnum.PARTIALLY_FILLED

    def _apply_order_terminal(self, shard: ShardState, event: VenueEventRow) -> None:
        """Clear command in-flight on terminal venue event (reject, cancel, expire).

        The venue may emit free-form ``status`` strings outside the
        ``TradeCommandStatusEnum`` value set (exchange-specific
        spellings like ``"canceled"``). The field stays typed as
        ``str | None`` so such values assign cleanly; callers that
        compare should use ``TradeCommandStatusEnum`` members which
        equal their underlying string value thanks to ``StrEnum``.
        """
        event_type = event["event_type"]
        if event_type == "order_rejected":
            shard.command.status = TradeCommandStatusEnum.REJECTED
        else:
            shard.command.status = event.get("status") or TradeCommandStatusEnum.CANCELLED
        shard.command.in_flight = False

    def restore_from_checkpoint(
        self,
        shard_key: str,
        position_qty: float,
        entry_price: float | None,
        cash: float,
        peak_equity: float,
        realized_pnl: float,
        turnover: float,
        last_venue_event_id: int,
        open_command_ids: list[str],
        seen_exec_ids: OrderedDict[str, None],
        position_opened_at: datetime | None = None,
    ) -> None:
        """Restore shard state from a checkpoint during recovery.

        Called at startup to fast-forward state from the last checkpoint
        before replaying delta VenueEvents.

        Args:
            shard_key: Unique identifier for the trading shard.
            position_qty: Net position quantity at checkpoint time.
            entry_price: Weighted average entry price, or None if flat.
            cash: Cash balance at checkpoint time.
            peak_equity: Historical peak equity watermark.
            realized_pnl: Cumulative realized profit and loss.
            turnover: Cumulative notional turnover.
            last_venue_event_id: Highest venue event ID already applied.
            open_command_ids: Public IDs of commands still in-flight.
            seen_exec_ids: Execution IDs already processed for dedup.
            position_opened_at: Venue timestamp at which the current
                open cycle was opened. Defaults to ``None`` so
                checkpoints written before the funding fee model shipped
                replay safely (NULL means: skip rate-locked accrual
                until the next zero-crossing stamps a fresh value).
        """
        shard = self._get_or_create_shard(shard_key)
        shard.position.position_qty = position_qty
        shard.position.entry_price = entry_price
        shard.position.realized_pnl = realized_pnl
        shard.position.position_opened_at = position_opened_at
        shard.cash = cash
        shard.peak_equity = peak_equity
        shard.turnover = turnover
        shard.last_venue_event_id = last_venue_event_id
        shard.seen_exec_ids = seen_exec_ids
        if open_command_ids:
            shard.command.in_flight = True
            shard.command.command_public_id = open_command_ids[-1]

    def mark_to_market(self, shard_key: str, price: float) -> float:
        """Update peak equity with current mark-to-market price.

        Args:
            shard_key: Unique identifier for the trading shard.
            price: Current mark-to-market price for the instrument.

        Returns:
            Current equity after mark-to-market adjustment.
        """
        shard = self._get_or_create_shard(shard_key)
        pos = shard.position
        unrealized = (
            pos.position_qty * price
            if not math.isclose(pos.position_qty, 0.0, abs_tol=1e-12)
            else 0.0
        )
        equity = shard.cash + unrealized
        if equity > shard.peak_equity:
            shard.peak_equity = equity
        return equity

    def halt_shard(self, shard_key: str, reason: str) -> None:
        """Halt a shard due to circuit breaker activation.

        The ``reason`` is recorded in the shard's ``halt_reasons`` set so a
        reason-scoped :meth:`unhalt_shard` can later release exactly this halt
        source without disturbing the others (e.g. the paired-execution guard
        releasing its halt while a reconciliation halt stays in force).

        Args:
            shard_key: Unique identifier for the trading shard.
            reason: Human-readable description of why the shard was halted;
                doubles as the selective un-halt key.
        """
        shard = self._get_or_create_shard(shard_key)
        shard.halted = True
        shard.halt_reasons.add(reason)
        logger.warning(f"TradeService: shard {shard_key} HALTED: {reason}")

    def unhalt_shard(self, shard_key: str, reason: str | None = None) -> None:
        """Un-halt a shard, either bluntly (operator) or for one reason only.

        Without a ``reason`` this is the operator-initiated full recovery:
        every halt reason is cleared, the shard un-halts, and the
        reconciliation failure counter resets — the operator has resolved the
        shard wholesale. With a ``reason`` (the paired-execution
        completion path) the release is FAIL-SAFE and scoped: only a reason
        that was actually registered is discarded, the shard un-halts only
        when NO reasons remain, and ``recon_failure_count`` is untouched — so
        an automated paired un-halt can never clear a reconciliation halt, an
        operator halt, or any halt set without a matching registered reason.

        Args:
            shard_key: Unique identifier for the trading shard.
            reason: The exact halt reason to release, or None for the blunt
                operator full clear.
        """
        shard = self._get_or_create_shard(shard_key)
        if reason is None:
            shard.halt_reasons.clear()
            shard.halted = False
            shard.recon_failure_count = 0
            logger.info(f"TradeService: shard {shard_key} un-halted")
            return
        if reason not in shard.halt_reasons:
            return
        shard.halt_reasons.discard(reason)
        if not shard.halt_reasons:
            shard.halted = False
            logger.info(f"TradeService: shard {shard_key} un-halted ({reason})")

    def shard_halt_reasons_with_prefix(self, prefix: str) -> list[tuple[str, str]]:
        """Return every (shard_key, halt_reason) pair whose reason has the prefix.

        Read model for the paired-execution quiet-halt sweep: it enumerates
        this coordinator's in-memory PAIRED halt reasons (by the canonical
        prefix) and releases exactly those whose scope no longer has an active
        durable halt — including halts cleared by ANOTHER coordinator or an
        operator, which this coordinator would otherwise never observe (the
        cleared row stops being listed). Reasons are sorted per shard for a
        deterministic result.

        Args:
            prefix: The reason-key prefix to match (e.g. the canonical
                paired-execution prefix).

        Returns:
            All matching (shard_key, reason) pairs across known shards.
        """
        return [
            (shard_key, reason)
            for shard_key, shard in self._shards.items()
            for reason in sorted(shard.halt_reasons)
            if reason.startswith(prefix)
        ]

    def record_recon_success(self, shard_key: str) -> None:
        """Record successful reconciliation, clearing failure counter.

        Args:
            shard_key: Unique identifier for the trading shard.
        """
        shard = self._get_or_create_shard(shard_key)
        shard.recon_failure_count = 0

    def record_recon_failure(self, shard_key: str, max_failures: int = 3) -> bool:
        """Record reconciliation failure. Returns True if shard should halt.

        Args:
            shard_key: Shard to record failure for.
            max_failures: Consecutive failures before halting.

        Returns:
            True if the failure count has reached the threshold.
        """
        shard = self._get_or_create_shard(shard_key)
        shard.recon_failure_count += 1
        if shard.recon_failure_count >= max_failures:
            self.halt_shard(
                shard_key, f"{shard.recon_failure_count} consecutive reconciliation failures"
            )
            return True
        return False

    def snapshot_for_checkpoint(
        self, shard_key: str
    ) -> dict[str, float | str | int | datetime | None]:
        """Return current shard state as a dict suitable for checkpoint persistence.

        Args:
            shard_key: Unique identifier for the trading shard.

        Returns:
            Dictionary containing position, cash, realized PnL,
            peak-equity, turnover, venue-event watermark,
            open-command metadata, and checkpoint timestamp fields ready
            for DB persistence.
        """
        shard = self._get_or_create_shard(shard_key)
        active_cmd_ids: list[str] = []
        if shard.command.in_flight and shard.command.command_public_id:
            active_cmd_ids.append(shard.command.command_public_id)
        return {
            "position_qty": shard.position.position_qty,
            "entry_price": shard.position.entry_price,
            "position_opened_at": shard.position.position_opened_at,
            "cash": shard.cash,
            "peak_equity": shard.peak_equity,
            "realized_pnl": shard.position.realized_pnl,
            "turnover": shard.turnover,
            "last_venue_event_id": shard.last_venue_event_id,
            "last_venue_event_at": datetime.now(UTC),
            "open_command_ids": json.dumps(active_cmd_ids) if active_cmd_ids else None,
            "seen_exec_ids": json.dumps(sorted(shard.seen_exec_ids)),
            "checkpoint_at": datetime.now(UTC),
        }
