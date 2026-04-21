"""TrailingStopEvaluator — trailing stop orders attached to position cycles.

An armed trailing stop ratchets the stop price as the market moves
favorably. When the price reverses through the stop level, it emits
a single reduce_only market close order. Stateful: peak_price and
current_stop are checkpointed every 10s by the service.

The stop activates immediately when min_lock_pct=0. With min_lock_pct>0,
trailing only begins after the price has moved min_lock_pct% in the
position's favor from entry_price (dead zone with no protection).

Recovery: restore_from_checkpoint floors peak at entry_price to prevent
premature triggering after a crash. Stale peak = wider stop = safer.
"""

from datetime import datetime
from typing import Any
from typing import cast

from snapper.application.plans.evaluator import PlanEvaluator
from snapper.core.json_types import JsonObject
from snapper.core.types import ExecutionPlanStatusEnum
from snapper.data.repository_types import ExecutionPlanRow
from snapper.messaging.schemas.data import ExecutionData
from snapper.messaging.schemas.data import TickData


class TrailingStopEvaluator(PlanEvaluator):
    """Evaluator for trailing stop plans on open position cycles.

    Lifecycle
        1. Plan created with status=armed via POST /api/trailing-stops
        2. on_tick ratchets peak_price and current_stop on favorable moves
        3. First stop breach emits a single reduce_only market close command
        4. Service transitions plan to active on command insert
        5. On child fill -> completed; on child rejection -> failed
    State
        peak_price: Highest (long) or lowest (short) price seen since armed.
        current_stop: Computed trailing stop level (0.0 = not yet activated).
    """

    def __init__(self) -> None:
        """Initialize with empty per-plan state dict."""
        self._state: dict[str, dict[str, float]] = {}

    async def on_tick(self, plan: ExecutionPlanRow, tick: TickData) -> list[JsonObject]:
        """Ratchet trailing stop and emit close on breach.

        Args:
            plan: Current plan state (must be armed to fire).
            tick: Incoming tick data with last price.

        Returns:
            List with one command dict on breach, empty otherwise.
        """
        if plan["status"] != ExecutionPlanStatusEnum.ARMED:
            return []
        last = tick.last
        if last is None:
            return []

        pid = plan["public_id"]
        state = self._state.setdefault(pid, {"peak_price": 0.0, "current_stop": 0.0})
        params = plan["params"]
        entry = float(cast(Any, params["entry_price"]))
        trail_pct = float(cast(Any, params["trailing_pct"])) / 100
        min_lock = float(cast(Any, params.get("min_lock_pct", 0))) / 100
        side = plan["side"]

        if side == "buy":
            return self._evaluate_long(state, last, entry, trail_pct, min_lock, plan)
        return self._evaluate_short(state, last, entry, trail_pct, min_lock, plan)

    def _evaluate_long(
        self,
        state: dict[str, float],
        last: float,
        entry: float,
        trail_pct: float,
        min_lock: float,
        plan: ExecutionPlanRow,
    ) -> list[JsonObject]:
        """Evaluate trailing stop for long positions (peak is highest)."""
        if state["peak_price"] < 1e-15:
            state["peak_price"] = max(last, entry)
        elif last > state["peak_price"]:
            state["peak_price"] = last
        if min_lock < 1e-15 or state["peak_price"] >= entry * (1 + min_lock):
            new_stop = state["peak_price"] * (1 - trail_pct)
            state["current_stop"] = max(state["current_stop"], new_stop)
        if state["current_stop"] > 0 and last <= state["current_stop"]:
            return [self._build_close_command(plan, last)]
        return []

    def _evaluate_short(
        self,
        state: dict[str, float],
        last: float,
        entry: float,
        trail_pct: float,
        min_lock: float,
        plan: ExecutionPlanRow,
    ) -> list[JsonObject]:
        """Evaluate trailing stop for short positions (peak is lowest)."""
        if state["peak_price"] < 1e-15:
            state["peak_price"] = min(last, entry)
        elif last < state["peak_price"]:
            state["peak_price"] = last
        if min_lock < 1e-15 or state["peak_price"] <= entry * (1 - min_lock):
            new_stop = state["peak_price"] * (1 + trail_pct)
            if state["current_stop"] < 1e-15:
                state["current_stop"] = new_stop
            else:
                state["current_stop"] = min(state["current_stop"], new_stop)
        if state["current_stop"] > 0 and last >= state["current_stop"]:
            return [self._build_close_command(plan, last)]
        return []

    def _build_close_command(self, plan: ExecutionPlanRow, trigger_price: float) -> JsonObject:
        """Build the reduce_only market close command."""
        closing_side = "sell" if plan["side"] == "buy" else "buy"
        return {
            "command_type": "create",
            "instrument": str(plan["params"]["native_instrument"]),
            "side": closing_side,
            "order_type": "market",
            "quantity": plan["total_quantity"],
            "price": None,
            "reduce_only": True,
            "leverage": plan["params"].get("leverage"),
            "trigger_type": "tick",
            "reason": "trailing_stop_hit",
            "trigger_price": trigger_price,
        }

    async def on_execution(
        self, plan: ExecutionPlanRow, execution: ExecutionData
    ) -> list[JsonObject]:
        """Trailing stops do not emit additional commands on fills.

        Args:
            plan: Current plan state.
            execution: Incoming execution data.

        Returns:
            Always empty list.
        """
        return []

    async def on_clock(self, plan: ExecutionPlanRow, now: datetime) -> list[JsonObject]:
        """Trailing stops are tick-driven only.

        Args:
            plan: Current plan state.
            now: Current UTC time.

        Returns:
            Always empty list.
        """
        return []

    def build_checkpoint_state(self, plan: ExecutionPlanRow) -> JsonObject:
        """Serialize peak_price and current_stop for checkpoint.

        Args:
            plan: Current plan state.

        Returns:
            Dict with peak_price and current_stop floats.
        """
        pid = plan["public_id"]
        state = self._state.get(pid, {"peak_price": 0.0, "current_stop": 0.0})
        return {"peak_price": state["peak_price"], "current_stop": state["current_stop"]}

    def restore_from_checkpoint(self, plan: ExecutionPlanRow, state: JsonObject) -> None:
        """Restore peak_price and current_stop, flooring peak at entry_price.

        After a restart, peak may be understated due to checkpoint staleness.
        Flooring at entry_price prevents the stop from resetting below entry
        (long: max(restored, entry), short: min(restored, entry) or entry if 0).

        Args:
            plan: Current plan state.
            state: Previously checkpointed state dict.
        """
        pid = plan["public_id"]
        restored_peak = float(cast(Any, state.get("peak_price", 0.0)))
        entry = float(cast(Any, plan["params"].get("entry_price", 0.0)))
        side = plan["side"]
        if side == "buy":
            restored_peak = max(restored_peak, entry)
        else:
            restored_peak = min(restored_peak, entry) if restored_peak > 0 else entry
        self._state[pid] = {
            "peak_price": restored_peak,
            "current_stop": float(cast(Any, state.get("current_stop", 0.0))),
        }

    def validate_params(self, params: JsonObject) -> None:
        """Validate trailing stop parameters.

        Args:
            params: Must contain trailing_pct, entry_price, native_instrument.

        Raises:
            ValueError: If required params missing or out of bounds.
        """
        if "trailing_pct" not in params:
            raise ValueError("trailing_pct required")
        if "native_instrument" not in params:
            raise ValueError("native_instrument required")
        if "entry_price" not in params:
            raise ValueError("entry_price required")
        pct = params["trailing_pct"]
        if not isinstance(pct, (int, float)) or pct <= 0 or pct >= 100:
            raise ValueError("trailing_pct must be between 0 (exclusive) and 100 (exclusive)")
        entry = params["entry_price"]
        if not isinstance(entry, (int, float)) or entry <= 0:
            raise ValueError("entry_price must be positive")
        min_lock = params.get("min_lock_pct", 0)
        if not isinstance(min_lock, (int, float)) or min_lock < 0 or min_lock >= 100:
            raise ValueError("min_lock_pct must be between 0 (inclusive) and 100 (exclusive)")

    def requires_capabilities(self) -> list[str]:
        """Trailing stops require reduce_only for the closing order.

        Returns:
            List containing supports_reduce_only flag name.
        """
        return ["supports_reduce_only"]
