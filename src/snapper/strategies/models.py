"""Strategy data models and configuration structures.

Provides the core data classes used across the strategy framework:
StrategySignal for trade signal emission and StrategyConfig for strategy
instance configuration.
"""

from dataclasses import dataclass
from dataclasses import field
from datetime import datetime
from typing import Any

from snapper.core.types import ExchangeEnum
from snapper.core.types import OrderExchange
from snapper.core.types import TradeSide
from snapper.infrastructure.symbols.functions import get_available_exchanges
from snapper.infrastructure.symbols.functions import is_tradeable


@dataclass
class StrategySignal:
    """Trading signal emitted by a strategy.

    Attributes:
        instrument: The trading instrument symbol.
        side: Trade direction ('buy' or 'sell').
        strength: Signal strength from 0.0 to 1.0.
        reason: Human-readable reason for the signal.
        price: Price at which signal was generated.
        timestamp: When the signal was generated (UTC datetime).
    """

    instrument: str
    side: TradeSide
    strength: float
    reason: str
    price: float
    timestamp: datetime | None = None


StrategySignalResult = StrategySignal | list[StrategySignal] | None
"""Return contract for strategy market-data callbacks.

A strategy callback (``on_candle`` / ``on_tick`` / ``on_trade``) may return:

- ``None`` — no signal this frame.
- a single :class:`StrategySignal` — the common single-leg case.
- a ``list[StrategySignal]`` — multiple legs the strategy emits together,
  in the order it chooses (the strategy decides which instrument goes
  first). Used by multi-leg strategies so both legs of a spread are
  emitted atomically instead of through a side-channel queue.

``BaseStrategy`` normalizes every callback return to ``list[StrategySignal]``
at the callback boundary and group-validates it (fail-closed) before any
signal is published or recorded.
"""


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
        wallet_public_id: Wallet that will execute orders for this strategy.
            Empty default for backwards compatibility; becomes required
            after the NOT NULL tightening migration lands.
        operator_public_id: Trading-identity operator that owns this
            strategy instance. Empty default; validated against the
            launching principal's ``operator_public_ids`` when populated.
    """

    name: str
    strategy_class: str
    inputs: list[str]
    outputs: list[str]
    exchange: OrderExchange = ExchangeEnum.PAPER
    params: dict[str, Any] = field(default_factory=dict)
    wallet_public_id: str = ""
    operator_public_id: str = ""

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
        if self.exchange != ExchangeEnum.PAPER:
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
