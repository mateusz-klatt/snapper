"""Validation and trusted sizing policy for live strategy orders."""

from dataclasses import dataclass
from decimal import Decimal
from math import isfinite
from typing import Annotated

from pydantic import BeforeValidator

from snapper.core.types import ExchangeEnum
from snapper.core.types import OrderExchange


def positive_order_cap(value: object) -> int | float:
    """Validate a positive finite JSON number without coercing its precision.

    Args:
        value: Raw cap from a strategy configuration.

    Returns:
        The validated numeric value.
    """
    if type(value) is int and value > 0:
        return value
    if type(value) is float and isfinite(value) and value > 0:
        return value
    raise ValueError("max_order_base_qty must be a positive finite number")


OrderBaseQuantityCap = Annotated[int | float, BeforeValidator(positive_order_cap)]


def validate_strategy_order_cap(exchange: OrderExchange, value: object) -> None:
    """Require a cap for every live strategy and validate any supplied cap.

    Args:
        exchange: Strategy execution venue.
        value: Configured order cap, if supplied.
    """
    if value is not None or exchange != ExchangeEnum.PAPER:
        positive_order_cap(value)


@dataclass(frozen=True)
class LiveStrategyOrderPolicy:
    """Identity and exact quantity limit resolved from an enabled strategy."""

    strategy_name: str
    wallet_public_id: str
    operator_public_id: str
    max_order_base_qty: Decimal
