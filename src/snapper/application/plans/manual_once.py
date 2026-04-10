"""ManualOnceEvaluator — simplest plan type, emits one command and completes.

A manual_once plan is created via POST /api/orders. The evaluator emits
a single TradeCommand at plan creation time and then transitions to
completed when the child order fills (or failed/cancelled on rejection).

No tick or clock callbacks — all behavior is fill-driven.
"""

from datetime import datetime

from snapper.application.plans.evaluator import PlanEvaluator
from snapper.core.json_types import JsonObject
from snapper.data.repository_types import ExecutionPlanRow
from snapper.messaging.schemas.data import ExecutionData
from snapper.messaging.schemas.data import TickData

_REQUIRED_PARAMS = {"order_type", "side"}
_VALID_ORDER_TYPES = {"market", "limit", "stop", "stop_limit"}


class ManualOnceEvaluator(PlanEvaluator):
    """Evaluator for manual_once plans (single command, fire-and-forget).

    Lifecycle:
        1. Plan created with status=pending
        2. PlanExecutorService calls emit_initial_command once
        3. Status transitions to active
        4. on_execution updates filled_quantity
        5. When fully filled → completed; on rejection → failed
    """

    async def on_tick(self, plan: ExecutionPlanRow, tick: TickData) -> list[JsonObject]:
        """Manual orders do not react to ticks.

        Args:
            plan: Current plan state (unused).
            tick: Incoming tick data (unused).

        Returns:
            Always empty list.
        """
        return []

    async def on_execution(
        self, plan: ExecutionPlanRow, execution: ExecutionData
    ) -> list[JsonObject]:
        """Track fill progress toward completion.

        Args:
            plan: Current plan state.
            execution: Incoming execution data.

        Returns:
            Always empty — manual orders do not spawn child commands
            from fills. Status transitions are handled by the service.
        """
        return []

    async def on_clock(self, plan: ExecutionPlanRow, now: datetime) -> list[JsonObject]:
        """Manual orders do not react to clock ticks.

        Args:
            plan: Current plan state (unused).
            now: Current UTC time (unused).

        Returns:
            Always empty list.
        """
        return []

    def build_checkpoint_state(self, plan: ExecutionPlanRow) -> JsonObject:
        """Manual orders have no evaluator state to checkpoint.

        Args:
            plan: Current plan state (unused).

        Returns:
            Empty dict.
        """
        return {}

    def restore_from_checkpoint(self, plan: ExecutionPlanRow, state: JsonObject) -> None:
        """Manual orders have no evaluator state to restore.

        Args:
            plan: Current plan state (unused).
            state: Previously checkpointed state dict (unused).
        """

    def validate_params(self, params: JsonObject) -> None:
        """Validate manual order parameters.

        Args:
            params: Must contain order_type and side. Optional: price,
                stop_price, time_in_force, post_only, leverage, reduce_only.

        Raises:
            ValueError: If required params missing or order_type invalid.
        """
        missing = _REQUIRED_PARAMS - set(params.keys())
        if missing:
            raise ValueError(f"Missing required params: {', '.join(sorted(missing))}")
        order_type = params.get("order_type", "")
        if order_type not in _VALID_ORDER_TYPES:
            raise ValueError(
                f"Invalid order_type={order_type!r}, "
                f"must be one of {sorted(_VALID_ORDER_TYPES)}"
            )
        if order_type in ("limit", "stop_limit") and params.get("price") is None:
            raise ValueError(f"order_type={order_type!r} requires price")
        if order_type in ("stop", "stop_limit") and params.get("stop_price") is None:
            raise ValueError(f"order_type={order_type!r} requires stop_price")

    def requires_capabilities(self) -> list[str]:
        """Manual orders have no special capability requirements.

        Returns:
            Empty list.
        """
        return []
