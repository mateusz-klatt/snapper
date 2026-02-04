"""Strategy data models and configuration structures.

Provides the core data classes used across the strategy framework:
Signal for trade signal emission and StrategyConfig for strategy
instance configuration.
"""

import logging
from collections.abc import Callable
from dataclasses import dataclass
from dataclasses import field
from typing import Any

from snapper.core.types import TradeSide
from snapper.infrastructure.symbols.functions import TradingExchange
from snapper.infrastructure.symbols.functions import get_available_exchanges
from snapper.infrastructure.symbols.functions import get_available_kraken_symbols
from snapper.infrastructure.symbols.functions import get_available_symbols
from snapper.infrastructure.symbols.functions import get_available_walutomat_symbols
from snapper.infrastructure.symbols.functions import get_available_zonda_symbols

logger = logging.getLogger(__name__)


@dataclass
class Signal:
    """Trading signal emitted by a strategy.

    Attributes:
        instrument: The trading instrument symbol.
        side: Trade direction ('buy' or 'sell').
        strength: Signal strength from 0.0 to 1.0.
        reason: Human-readable reason for the signal.
        price: Price at which signal was generated.
        timestamp: Signal generation timestamp (Unix epoch).
        metadata: Additional signal metadata.
    """

    instrument: str
    side: TradeSide
    strength: float
    reason: str
    price: float
    timestamp: float | None = None
    metadata: dict[str, Any] = field(default_factory=dict)


def _get_exchange_instrument_resolver(
    exchange: str,
) -> tuple[Callable[[], list[str]], str] | None:
    """Look up the instrument resolver and label for an exchange.

    Args:
        exchange: Exchange name to look up.

    Returns:
        Tuple of (resolver_callable, label) or None if exchange unknown.
    """
    resolvers: dict[str, tuple[Callable[[], list[str]], str]] = {
        "paper": (get_available_symbols, "Paper (all exchanges)"),
        "walutomat": (get_available_walutomat_symbols, "Walutomat (FX pairs only)"),
        "zonda": (get_available_zonda_symbols, "Zonda"),
        "kraken": (get_available_kraken_symbols, "Kraken"),
    }
    return resolvers.get(exchange)


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
    exchange: TradingExchange = "paper"
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
        """Validate that output instruments are valid for the exchange."""
        resolver = _get_exchange_instrument_resolver(self.exchange)
        if resolver is None:
            return
        get_instruments, exchange_label = resolver
        valid_instruments = get_instruments()
        if not valid_instruments:
            logger.warning(
                f"Strategy {self.name}: No symbols loaded for {exchange_label}, "
                "skipping output validation (DB may be empty)"
            )
            return
        invalid_instruments = [inst for inst in self.outputs if inst not in valid_instruments]
        if invalid_instruments:
            raise ValueError(
                f"Strategy {self.name}: Invalid output instruments for {exchange_label}: "
                f"{invalid_instruments}. Valid instruments: {valid_instruments[:20]}..."
            )
