"""Strategy data models and configuration structures.

Provides the core data classes used across the strategy framework:
Signal for trade signal emission and StrategyConfig for strategy
instance configuration.
"""

from dataclasses import dataclass
from dataclasses import field
from datetime import datetime
from typing import Any

from snapper.core.types import OrderExchange
from snapper.core.types import TradeSide
from snapper.infrastructure.symbols.functions import get_available_exchanges
from snapper.infrastructure.symbols.functions import is_tradeable


@dataclass
class Signal:
    """Trading signal emitted by a strategy.

    Attributes:
        instrument: The trading instrument symbol.
        side: Trade direction ('buy' or 'sell').
        strength: Signal strength from 0.0 to 1.0.
        reason: Human-readable reason for the signal.
        price: Price at which signal was generated.
        timestamp: When the signal was generated (UTC datetime).
        metadata: Additional signal metadata.
    """

    instrument: str
    side: TradeSide
    strength: float
    reason: str
    price: float
    timestamp: datetime | None = None
    metadata: dict[str, Any] = field(default_factory=dict)


@dataclass
class StrategyConfig:
    """Configuration for a trading strategy.

    Attributes:
        name: Unique strategy instance name.
        strategy_class: Name of the strategy class to instantiate.
        inputs: List of ZMQ topics to subscribe to.
        outputs: List of output instruments for signals.
        exchange: Target exchange for order execution.
        params: Strategy-specific parameters.
    """

    name: str
    strategy_class: str
    inputs: list[str]
    outputs: list[str]
    exchange: OrderExchange = "paper"
    params: dict[str, Any] = field(default_factory=dict)

    @staticmethod
    def _is_paper_or_replay(topic: str) -> bool:
        """Check if a topic string refers to paper or replay data.

        Args:
            topic: Input topic string.

        Returns:
            True if the topic contains 'paper' or 'replay'.
        """
        lowered = topic.lower()
        return "paper" in lowered or "replay" in lowered

    def _validate_paper_inputs(self) -> None:
        """Validate paper/replay input consistency with exchange setting."""
        has_paper_input = any(self._is_paper_or_replay(inp) for inp in self.inputs)
        if not has_paper_input:
            return
        if self.exchange != "paper":
            raise ValueError(
                f"Strategy {self.name}: Paper/replay input data MUST use exchange='paper'. "
                f"Found inputs: {self.inputs}, exchange: {self.exchange}"
            )
        all_paper = all(self._is_paper_or_replay(inp) for inp in self.inputs)
        if not all_paper:
            raise ValueError(
                f"Strategy {self.name}: Cannot mix paper/replay and live inputs. "
                f"All inputs must be paper/replay OR all must be live (no mixing). "
                f"Found inputs: {self.inputs}"
            )

    def __post_init__(self) -> None:
        """Validate configuration after initialization."""
        if not self.name:
            raise ValueError("Strategy name cannot be empty")
        if not self.inputs:
            raise ValueError(f"Strategy {self.name} must have at least one input")
        if not self.outputs:
            raise ValueError(f"Strategy {self.name} must define at least one output instrument")
        valid_exchanges = get_available_exchanges()
        if self.exchange not in valid_exchanges:
            raise ValueError(
                f"Strategy {self.name}: exchange must be one of {valid_exchanges}, "
                f"got '{self.exchange}'"
            )
        self._validate_output_instruments()
        self._validate_paper_inputs()

    def _validate_output_instruments(self) -> None:
        """Validate that output instruments are tradeable on the configured exchange.

        For live exchanges, each output instrument must have a capability row
        with ``can_trade=True``. For paper exchange, ``is_tradeable`` checks
        that the symbol has at least one alias in any forward map (paper has
        no rows in symbol_exchange_capabilities but can trade any known
        symbol). An empty tradeable set is treated as a hard error (fail-fast)
        to prevent strategies from entering trading mode without capability
        data loaded.
        """
        non_tradeable = [inst for inst in self.outputs if not is_tradeable(inst, self.exchange)]
        if non_tradeable:
            raise ValueError(
                f"Strategy {self.name}: instruments not tradeable on "
                f"{self.exchange}: {non_tradeable}"
            )
