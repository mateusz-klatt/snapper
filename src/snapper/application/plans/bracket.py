"""BracketEvaluator — SL/TP bracket orders attached to position cycles.

An armed bracket watches ticks for its instrument. On the first SL or TP
threshold breach it emits a single reduce_only market close order and
transitions to active (via service fill tracking). Brackets are stateless:
trigger thresholds live in plan.params (immutable after create).

Brackets require ``supports_reduce_only`` on the venue.
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

_REQUIRED_PARAMS = {"native_instrument"}
_VALID_LEGS = {"sl_price", "tp_price"}


def _triggered_leg_for_price(
    side: str,
    stop_loss: float | None,
    take_profit: float | None,
    last_price: float,
) -> str | None:
    """Return the triggered leg name for the latest tick price."""
    if side == "buy":
        if stop_loss is not None and stop_loss >= last_price:
            return "sl_hit"
        if take_profit is not None and take_profit <= last_price:
            return "tp_hit"
        return None
    if stop_loss is not None and stop_loss <= last_price:
        return "sl_hit"
    if take_profit is not None and take_profit >= last_price:
        return "tp_hit"
    return None


def _build_close_command(
    plan: ExecutionPlanRow,
    params: JsonObject,
    side: str,
    triggered_leg: str,
) -> JsonObject:
    """Build the reduce-only market close command for a triggered bracket."""
    closing_side = "sell" if side == "buy" else "buy"
    return {
        "command_type": "create",
        "instrument": str(params["native_instrument"]),
        "side": closing_side,
        "order_type": "market",
        "quantity": plan["total_quantity"],
        "price": None,
        "reduce_only": True,
        "leverage": params.get("leverage"),
        "trigger_type": "tick",
        "reason": triggered_leg,
    }


class BracketEvaluator(PlanEvaluator):
    """Evaluator for bracket plans (SL/TP on open position cycles).

    Lifecycle:
        1. Plan created with status=armed via POST /api/execution-plans
        2. on_tick checks SL/TP thresholds against tick.last
        3. First breach emits a single reduce_only market close command
        4. Service transitions plan to active on command insert
        5. On child fill → completed; on child rejection → failed
    """

    async def on_tick(self, plan: ExecutionPlanRow, tick: TickData) -> list[JsonObject]:
        """Check SL/TP thresholds and emit closing command on first breach.

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
        params = plan["params"]
        sl = params.get("sl_price")
        tp = params.get("tp_price")
        side = plan["side"]

        sl_f = float(cast(Any, sl)) if sl is not None else None
        tp_f = float(cast(Any, tp)) if tp is not None else None
        triggered_leg = _triggered_leg_for_price(side, sl_f, tp_f, last)
        if triggered_leg is None:
            return []
        return [_build_close_command(plan, params, side, triggered_leg)]

    async def on_execution(
        self, plan: ExecutionPlanRow, execution: ExecutionData
    ) -> list[JsonObject]:
        """Brackets do not emit additional commands on fills.

        Fill tracking and status transitions are handled by the service
        (_handle_execution). Exchange reduce_only clamping means the fill
        may be smaller than total_quantity — the service completes the
        bracket on any child terminal status, not on qty match.

        Args:
            plan: Current plan state.
            execution: Incoming execution data.

        Returns:
            Always empty list.
        """
        return []

    async def on_clock(self, plan: ExecutionPlanRow, now: datetime) -> list[JsonObject]:
        """Brackets are tick-driven only. No clock behavior.

        Args:
            plan: Current plan state (unused).
            now: Current UTC time (unused).

        Returns:
            Always empty list.
        """
        return []

    def build_checkpoint_state(self, plan: ExecutionPlanRow) -> JsonObject:
        """Brackets have no evaluator-owned state beyond plan params.

        Trigger thresholds are in plan.params (immutable after create).

        Args:
            plan: Current plan state (unused).

        Returns:
            Empty dict.
        """
        return {}

    def restore_from_checkpoint(self, plan: ExecutionPlanRow, state: JsonObject) -> None:
        """Nothing to restore for stateless bracket evaluator.

        Args:
            plan: Current plan state (unused).
            state: Previously checkpointed state dict (unused).
        """

    def validate_params(self, params: JsonObject) -> None:
        """Validate bracket parameters.

        At least one of sl_price or tp_price must be present.
        native_instrument is required for command emission.

        Args:
            params: Must contain native_instrument and at least one
                of sl_price/tp_price.

        Raises:
            ValueError: If required params missing or no legs specified.
        """
        missing = _REQUIRED_PARAMS - set(params.keys())
        if missing:
            raise ValueError(f"Missing required params: {', '.join(sorted(missing))}")
        has_sl = params.get("sl_price") is not None
        has_tp = params.get("tp_price") is not None
        if not has_sl and not has_tp:
            raise ValueError("At least one of sl_price or tp_price required")

    def requires_capabilities(self) -> list[str]:
        """Brackets require reduce_only support.

        Returns:
            List containing supports_reduce_only flag name.
        """
        return ["supports_reduce_only"]
