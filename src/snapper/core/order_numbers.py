"""Numeric transport and storage constraints shared by manual-order entry points."""

from typing import Annotated

from pydantic import Field
from pydantic import TypeAdapter

from snapper.core.json_types import JsonObject

PositiveOrderNumber = Annotated[float, Field(strict=True, gt=0, allow_inf_nan=False)]
OrderLeverage = Annotated[int, Field(strict=True, gt=0, le=2147483647)]

_ORDER_NUMBER = TypeAdapter(PositiveOrderNumber)
_ORDER_LEVERAGE = TypeAdapter(OrderLeverage)


def validate_manual_order_numbers(params: JsonObject) -> None:
    """Reject unsafe supplied numbers while preserving stored-plan omissions.

    Quantity lives on the execution-plan row, so stored parameter mappings
    may omit it. An explicitly supplied quantity must be finite and positive.
    Optional prices and leverage retain their nullable transport contract.
    Leverage is bounded by the signed four-byte persistence column; this is
    not an instrument capability or trading-policy limit.

    Args:
        params: Raw manual-order parameters from an entry point or stored plan.

    Raises:
        ValueError: If a supplied amount or leverage violates its contract.
    """
    if "quantity" in params:
        _ORDER_NUMBER.validate_python(params["quantity"])
    for field in ("price", "stop_price"):
        value = params.get(field)
        if value is not None:
            _ORDER_NUMBER.validate_python(value)
    leverage = params.get("leverage")
    if leverage is not None:
        _ORDER_LEVERAGE.validate_python(leverage)
