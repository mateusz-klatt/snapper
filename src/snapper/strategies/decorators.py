"""Strategy decorators for registration and process creation.

This module provides decorators for registering strategy classes
and creating process wrappers for running strategies.
"""

from collections.abc import Callable
from typing import Any
from typing import TypeVar

from snapper.strategies.base import BaseStrategy
from snapper.strategies.factory import StrategyFactory
from snapper.strategies.process_wrapper import create_strategy_process as _create_strategy_process

T = TypeVar("T", bound=type[BaseStrategy])


def register_strategy(name: str | None = None) -> Callable[[T], T]:
    """Decorator to register a strategy class in the factory.

    Args:
        name: Optional name to register under. Defaults to class name.

    Returns:
        Decorator function.

    Example:
        @register_strategy("MyStrategy")
        class MyStrategy(BaseStrategy):
            ...
    """

    def decorator(cls: T) -> T:
        strategy_name = name or cls.__name__
        StrategyFactory.register_strategy_class(strategy_name, cls)
        return cls

    return decorator


def create_strategy_process(
    process_name: str,
    default_config: dict[str, Any],
) -> Callable[[T], T]:
    """Decorator to create a process wrapper for a strategy.

    Registers the strategy as a managed process in the process manager.

    Args:
        process_name: Name for the process.
        default_config: Default configuration dictionary.

    Returns:
        Decorator function.

    Example:
        @create_strategy_process(
            process_name="strategy_rsi",
            default_config={"name": "rsi", ...}
        )
        class RSIStrategy(BaseStrategy):
            ...
    """

    def decorator(cls: T) -> T:
        strategy_class_name = cls.__name__
        _ = _create_strategy_process(
            process_name=process_name,
            strategy_class=strategy_class_name,
            default_config=default_config,
        )
        return cls

    return decorator
