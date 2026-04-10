"""PlanEvaluator abstract base class for execution plan evaluators.

Each execution plan type (manual_once, bracket, trailing_stop, peg,
scheduler) has a corresponding PlanEvaluator implementation that
decides when to emit TradeCommands. The PlanExecutorService dispatches
market events to the correct evaluator instance for each active plan.
"""

from abc import ABC
from abc import abstractmethod
from datetime import datetime

from snapper.core.json_types import JsonObject
from snapper.data.repository_types import ExecutionPlanRow
from snapper.messaging.schemas.data import ExecutionData
from snapper.messaging.schemas.data import TickData


class PlanEvaluator(ABC):
    """Abstract evaluator driving execution plan behavior.

    Each plan type has exactly one PlanEvaluator subclass. The evaluator
    receives market events (ticks, executions, clock) and returns a list
    of command dicts to emit, or an empty list to skip.

    Evaluators store per-plan in-memory state that is periodically
    checkpointed via build_checkpoint_state / restore_from_checkpoint.
    """

    @abstractmethod
    async def on_tick(self, plan: ExecutionPlanRow, tick: TickData) -> list[JsonObject]:
        """React to a tick event for the plan's instrument.

        Args:
            plan: Current plan state.
            tick: Incoming tick data.

        Returns:
            List of command dicts to emit (empty = no action).
        """
        ...

    @abstractmethod
    async def on_execution(
        self, plan: ExecutionPlanRow, execution: ExecutionData
    ) -> list[JsonObject]:
        """React to an execution (fill) event.

        Args:
            plan: Current plan state.
            execution: Incoming execution data.

        Returns:
            List of command dicts to emit (empty = no action).
        """
        ...

    @abstractmethod
    async def on_clock(self, plan: ExecutionPlanRow, now: datetime) -> list[JsonObject]:
        """React to a 1Hz clock tick.

        Args:
            plan: Current plan state.
            now: Current UTC time.

        Returns:
            List of command dicts to emit (empty = no action).
        """
        ...

    @abstractmethod
    def build_checkpoint_state(self, plan: ExecutionPlanRow) -> JsonObject:
        """Serialize evaluator in-memory state for checkpointing.

        Args:
            plan: Current plan state.

        Returns:
            JSON-serializable state dict.
        """
        ...

    @abstractmethod
    def restore_from_checkpoint(self, plan: ExecutionPlanRow, state: JsonObject) -> None:
        """Restore evaluator in-memory state from a checkpoint.

        Args:
            plan: Current plan state.
            state: Previously checkpointed state dict.
        """
        ...

    @abstractmethod
    def validate_params(self, params: JsonObject) -> None:
        """Validate plan-type-specific parameters.

        Args:
            params: Plan params dict to validate.

        Raises:
            ValueError: If params are invalid for this plan type.
        """
        ...

    @abstractmethod
    def requires_capabilities(self) -> list[str]:
        """Return capability flag names required for this plan type.

        Returns:
            List of InstrumentOrderCapability flag names
            (e.g., ['supports_post_only']).
        """
        ...
