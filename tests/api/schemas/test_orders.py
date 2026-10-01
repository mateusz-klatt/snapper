"""Numeric constraints for manual order request bodies."""

import pytest
from pydantic import ValidationError

from snapper.api.schemas.orders import CreateOrderBody
from snapper.core.json_types import JsonObject
from snapper.core.json_types import JsonValue


@pytest.mark.parametrize("field", ["quantity", "price", "stop_price"])
@pytest.mark.parametrize(
    "value", [0, -1, float("nan"), float("inf"), -float("inf"), True, "1", 10**1000]
)
def test_invalid_order_numbers(field: str, value: JsonValue) -> None:
    """Reject unsafe amounts before a request can reach the repository.

    Given: an order body with an unsafe quantity or price.
    When: the request schema validates the body.
    Then: validation rejects the unsafe amount.
    """
    payload: JsonObject = {
        "instrument": "BTC-USD",
        "instrument_public_id": "instrument-1",
        "exchange": "kraken",
        "side": "buy",
        "order_type": "stop_limit",
        "quantity": 1,
        "price": 2,
        "stop_price": 3,
    }
    payload[field] = value
    with pytest.raises(ValidationError):
        CreateOrderBody.model_validate(payload)


@pytest.mark.parametrize(
    "value", [0, -1, True, False, 1.5, 1.0, "1", 2147483648, float("inf"), float("nan")]
)
def test_invalid_order_leverage(value: JsonValue) -> None:
    """Leverage must fit a strict positive signed four-byte integer.

    Given: an order body with invalid leverage.
    When: the request schema validates the body.
    Then: validation rejects leverage outside the strict positive int32 contract.
    """
    with pytest.raises(ValidationError):
        CreateOrderBody.model_validate(
            {
                "instrument": "BTC-USD",
                "instrument_public_id": "instrument-1",
                "exchange": "kraken",
                "side": "buy",
                "order_type": "market",
                "quantity": 1,
                "leverage": value,
            }
        )


@pytest.mark.parametrize("leverage", [None, 1, 2147483647])
def test_valid_order_numbers(leverage: int | None) -> None:
    """Accept integer amounts, optional nulls, and both leverage boundaries.

    Given: a positive quantity, null prices, and valid optional leverage.
    When: the request schema validates the body.
    Then: the quantity and leverage retain their values.
    """
    body = CreateOrderBody.model_validate(
        {
            "instrument": "BTC-USD",
            "instrument_public_id": "instrument-1",
            "exchange": "kraken",
            "side": "buy",
            "order_type": "market",
            "quantity": 1,
            "price": None,
            "stop_price": None,
            "leverage": leverage,
        }
    )
    assert body.quantity == 1
    assert body.leverage == leverage
