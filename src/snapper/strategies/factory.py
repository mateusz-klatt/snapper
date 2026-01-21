"""Strategy factory and registry.

This module provides the factory pattern for creating and managing
trading strategy instances with output conflict detection.
"""

from typing import Any

from snapper.strategies.base import BaseStrategy
from snapper.strategies.base import StrategyConfig


class StrategyNotFoundError(Exception):
    """Raised when a requested strategy is not found."""

    pass


class StrategyFactory:
    """Factory for creating and managing strategy instances.

    Maintains a registry of strategy classes and active instances,
    preventing output topic conflicts between strategies.

    Attributes:
        STRATEGY_CLASSES: Class-level registry of strategy classes.
    """

    STRATEGY_CLASSES: dict[str, type[BaseStrategy]] = {}

    @classmethod
    def register_strategy_class(cls, name: str, strategy_class: type[BaseStrategy]) -> None:
        """Register a strategy class in the factory registry.

        Args:
            name: The name to register the strategy under.
            strategy_class: The strategy class to register.
        """
        cls.STRATEGY_CLASSES[name] = strategy_class

    def __init__(self) -> None:
        """Initialize the strategy factory."""
        self._active_strategies: dict[str, BaseStrategy] = {}
        self._active_outputs: dict[str, str] = {}

    def create_strategy(self, config: StrategyConfig) -> BaseStrategy:
        """Create a new strategy instance from configuration.

        Args:
            config: Strategy configuration.

        Returns:
            The created strategy instance.

        Raises:
            KeyError: If strategy class is not registered.
            ValueError: If strategy name already exists or output conflict.
        """
        if config.strategy_class not in self.STRATEGY_CLASSES:
            available = ", ".join(self.STRATEGY_CLASSES.keys())
            raise KeyError(
                f"Unknown strategy class '{config.strategy_class}'. "
                f"Available strategies: {available}"
            )
        if config.name in self._active_strategies:
            raise ValueError(f"Strategy with name '{config.name}' already exists")
        strategy_class = self.STRATEGY_CLASSES[config.strategy_class]
        strategy = strategy_class(config)
        for topic in strategy.output_topics:
            if topic in self._active_outputs:
                existing_strategy = self._active_outputs[topic]
                raise ValueError(
                    f"Output topic '{topic}' already used by strategy "
                    f"'{existing_strategy}'. Cannot create strategy '{config.name}' "
                    f"with duplicate output topic to prevent conflicts."
                )
        self._active_strategies[config.name] = strategy
        for topic in strategy.output_topics:
            self._active_outputs[topic] = config.name
        return strategy

    async def start_strategy(self, name: str) -> None:
        """Start a registered strategy.

        Args:
            name: The strategy name.

        Raises:
            StrategyNotFoundError: If strategy is not found.
        """
        if name not in self._active_strategies:
            raise StrategyNotFoundError(f"Strategy '{name}' not found")
        await self._active_strategies[name].start()

    async def stop_strategy(self, name: str) -> None:
        """Stop and remove a strategy.

        Args:
            name: The strategy name.

        Raises:
            StrategyNotFoundError: If strategy is not found.
        """
        if name not in self._active_strategies:
            raise StrategyNotFoundError(f"Strategy '{name}' not found")
        strategy = self._active_strategies[name]
        await strategy.stop()
        del self._active_strategies[name]
        for topic in strategy.output_topics:
            del self._active_outputs[topic]

    async def stop_all(self) -> None:
        """Stop all active strategies."""
        for name in list(self._active_strategies.keys()):
            await self.stop_strategy(name)

    def get_strategy(self, name: str) -> BaseStrategy:
        """Get an active strategy by name.

        Args:
            name: The strategy name.

        Returns:
            The strategy instance.

        Raises:
            StrategyNotFoundError: If strategy is not found.
        """
        if name not in self._active_strategies:
            raise StrategyNotFoundError(f"Strategy '{name}' not found")
        return self._active_strategies[name]

    def list_strategies(self) -> dict[str, dict[str, Any]]:
        """List all active strategies with their configurations.

        Returns:
            Dictionary mapping strategy names to their details.
        """
        return {
            name: {
                "class": strategy.config.strategy_class,
                "inputs": strategy.config.inputs,
                "outputs": strategy.config.outputs,
                "output_topics": strategy.output_topics,
                "exchange": strategy.exchange,
                "params": strategy.config.params,
                "running": strategy.is_running,
            }
            for name, strategy in self._active_strategies.items()
        }

    def validate_no_output_conflicts(self, configs: list[StrategyConfig]) -> list[str]:
        """Validate that strategy configs have no output topic conflicts.

        Args:
            configs: List of strategy configurations to validate.

        Returns:
            List of error messages for any conflicts found.
        """
        errors: list[str] = []
        outputs_seen: dict[str, str] = {}
        for config in configs:
            for instrument in config.outputs:
                if config.exchange == "paper":
                    topic = f"signals.paper.{instrument}.{config.name}"
                else:
                    topic = f"signals.{config.exchange}.{instrument}.live"
                if topic in outputs_seen:
                    errors.append(
                        f"Output topic conflict: '{config.name}' and '{outputs_seen[topic]}' "
                        f"both use output topic '{topic}'"
                    )
                else:
                    outputs_seen[topic] = config.name
        return errors


strategy_factory = StrategyFactory()
