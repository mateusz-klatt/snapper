"""Trade domain service for in-memory trade runtime projections.

TradeService consumes VenueEvent-shaped rows and updates per-shard
command state, position state, cash and turnover state, execution
deduplication, and reconciliation circuit-breaker counters. It exposes
the live read model used inside the trade runtime and produces snapshots
that TraderCoordinator persists as TradeProjectionCheckpoint rows.
Canonical Order and Execution rows are currently persisted on the
executor/exchange-client path.
"""

import json
import math
from dataclasses import dataclass
from dataclasses import field
from datetime import UTC
from datetime import datetime
from typing import Final

from loguru import logger

from snapper.data.repository_types import TradeCommandRow
from snapper.data.repository_types import VenueEventRow

TERMINAL_STATUSES: Final[frozenset[str]] = frozenset(
    {"filled", "cancelled", "expired", "rejected", "failed"}
)

FILL_EVENT_TYPES: Final[frozenset[str]] = frozenset({"fill_observed"})


@dataclass
class PositionProjection:
    """In-memory projection of position state for a single shard.

    Updated on every confirmed fill. Read by TradingEngineService for
    sizing, risk, and stop-loss decisions.
    """

    position_qty: float = 0.0
    entry_price: float | None = None
    realized_pnl: float = 0.0


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
    """Aggregate in-memory state for a single shard_key."""

    position: PositionProjection = field(default_factory=PositionProjection)
    command: CommandState = field(default_factory=CommandState)
    cash: float = 10_000.0
    peak_equity: float = 10_000.0
    turnover: float = 0.0
    last_venue_event_id: int = 0
    seen_exec_ids: set[str] = field(default_factory=set)
    halted: bool = False
    recon_failure_count: int = 0


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

    def get_position(self, shard_key: str) -> PositionProjection:
        """Read model: current position for engine sizing/risk decisions.

        Args:
            shard_key: Unique identifier for the trading shard.

        Returns:
            PositionProjection with current quantity, entry price, and
            realized PnL.
        """
        return self._get_or_create_shard(shard_key).position

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

        Called by the outbox dispatcher after writing TradeCommand to DB.
        Sets the command state to in-flight.

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
        fill_observed, order_terminal. Updates position, command state,
        cash, and watermark.

        Args:
            event: Venue event row to apply. Must contain shard_key,
                event_type, and id fields at minimum.
        """
        shard_key = event["shard_key"]
        shard = self._get_or_create_shard(shard_key)
        event_type = event["event_type"]
        event_id = event["id"]

        if event_id <= shard.last_venue_event_id:
            return

        if event_type == "order_accepted":
            self._apply_order_accepted(shard, event)
        elif event_type == "order_rejected":
            self._apply_order_terminal(shard, event)
        elif event_type == "fill_observed":
            self._apply_fill(shard, event)
        elif event_type == "order_terminal":
            self._apply_order_terminal(shard, event)
        else:
            logger.warning(f"TradeService: unknown venue event type: {event_type}")

        shard.last_venue_event_id = event_id

    def _apply_order_accepted(self, shard: ShardState, event: VenueEventRow) -> None:
        """Update command state on venue acceptance."""
        shard.command.status = "accepted"
        shard.command.exchange_order_id = event.get("exchange_order_id")

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

        if side_lower in ("buy", "sell"):
            signed_qty = fill_size if side_lower == "buy" else -fill_size
            self._update_position(shard.position, signed_qty, fill_size, fill_price)
            self._update_cash(shard, side_lower, notional, fee)

        shard.turnover += notional
        self._update_command_fill_status(shard, event)

    def _dedup_fill(self, shard: ShardState, event: VenueEventRow) -> bool:
        """Return True if the fill is new and should be applied.

        Adds exec_id and trade_id to the seen set for future dedup.
        """
        exec_id = event.get("exec_id")
        trade_id = event.get("trade_id")
        dedup_key = exec_id or trade_id or f"fallback-{event['id']}"
        if dedup_key in shard.seen_exec_ids:
            return False
        if exec_id:
            shard.seen_exec_ids.add(exec_id)
        if trade_id:
            shard.seen_exec_ids.add(trade_id)
        return True

    def _update_position(
        self,
        pos: PositionProjection,
        signed_qty: float,
        fill_size: float,
        fill_price: float,
    ) -> None:
        """Update position quantity and entry price for a fill."""
        is_increasing = (pos.position_qty >= 0 and signed_qty > 0) or (
            pos.position_qty <= 0 and signed_qty < 0
        )
        if is_increasing:
            self._increase_position(pos, fill_size, fill_price)
        else:
            self._decrease_position(pos, fill_size, fill_price)

        pos.position_qty += signed_qty
        if abs(pos.position_qty) < 1e-12:
            pos.position_qty = 0.0
            pos.entry_price = None

    @staticmethod
    def _increase_position(pos: PositionProjection, fill_size: float, fill_price: float) -> None:
        """Recalculate weighted-average entry price for a position-increasing fill."""
        old_qty = abs(pos.position_qty)
        new_qty = old_qty + fill_size
        if pos.entry_price is not None and old_qty > 0 and new_qty > 0:
            pos.entry_price = (old_qty * pos.entry_price + fill_size * fill_price) / new_qty
        else:
            pos.entry_price = fill_price

    @staticmethod
    def _decrease_position(pos: PositionProjection, fill_size: float, fill_price: float) -> None:
        """Realize PnL and handle overshoot for a position-decreasing fill."""
        close_qty = min(fill_size, abs(pos.position_qty))
        overshoot = fill_size - close_qty
        if pos.entry_price is not None and close_qty > 0:
            pnl_per_unit = fill_price - pos.entry_price
            if pos.position_qty < 0:
                pnl_per_unit = pos.entry_price - fill_price
            pos.realized_pnl += close_qty * pnl_per_unit
        if overshoot > 1e-12:
            pos.entry_price = fill_price

    @staticmethod
    def _update_cash(shard: ShardState, side: str, notional: float, fee: float) -> None:
        """Adjust cash balance for a buy or sell fill."""
        if side == "buy":
            shard.cash -= notional + fee
        else:
            shard.cash += notional - fee

    def _update_command_fill_status(self, shard: ShardState, event: VenueEventRow) -> None:
        """Update command FSM based on fill status field."""
        status = event.get("status")
        if status == "filled":
            shard.command.status = "filled"
            shard.command.in_flight = False
        elif status == "partial":
            shard.command.status = "partially_filled"

    def _apply_order_terminal(self, shard: ShardState, event: VenueEventRow) -> None:
        """Clear command in-flight on terminal venue event (reject, cancel, expire)."""
        event_type = event["event_type"]
        if event_type == "order_rejected":
            shard.command.status = "rejected"
        else:
            status = event.get("status") or "cancelled"
            shard.command.status = status
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
        seen_exec_ids: set[str],
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
        """
        shard = self._get_or_create_shard(shard_key)
        shard.position.position_qty = position_qty
        shard.position.entry_price = entry_price
        shard.position.realized_pnl = realized_pnl
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

        Args:
            shard_key: Unique identifier for the trading shard.
            reason: Human-readable description of why the shard was halted.
        """
        shard = self._get_or_create_shard(shard_key)
        shard.halted = True
        logger.warning(f"TradeService: shard {shard_key} HALTED: {reason}")

    def unhalt_shard(self, shard_key: str) -> None:
        """Un-halt a shard (operator-initiated recovery).

        Args:
            shard_key: Unique identifier for the trading shard.
        """
        shard = self._get_or_create_shard(shard_key)
        shard.halted = False
        shard.recon_failure_count = 0
        logger.info(f"TradeService: shard {shard_key} un-halted")

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
            "cash": shard.cash,
            "peak_equity": shard.peak_equity,
            "realized_pnl": shard.position.realized_pnl,
            "turnover": shard.turnover,
            "last_venue_event_id": shard.last_venue_event_id,
            "last_venue_event_at": datetime.now(UTC),
            "open_command_ids": json.dumps(active_cmd_ids) if active_cmd_ids else None,
            "checkpoint_at": datetime.now(UTC),
        }
