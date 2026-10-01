"""Adversarial numeric validation for direct manual-plan ingestion."""

import pytest

from snapper.application.plans.manual_once import ManualOnceEvaluator
from snapper.core.json_types import JsonObject
from snapper.core.json_types import JsonValue


@pytest.mark.parametrize("field", ["quantity", "price", "stop_price"])
@pytest.mark.parametrize(
    "value", [0, -1, float("nan"), float("inf"), -float("inf"), True, "1", [], {}, 10**1000]
)
def test_invalid_plan_numbers(field: str, value: JsonValue) -> None:
    """Refuse invalid raw values even when callers bypass transport parsing.

    Given: manual-plan parameters containing an unsafe amount.
    When: the evaluator validates the parameters directly.
    Then: validation raises ValueError.
    """
    params: JsonObject = {"order_type": "stop_limit", "side": "buy", "price": 1, "stop_price": 2}
    params[field] = value
    evaluator = ManualOnceEvaluator()
    with pytest.raises(ValueError):
        evaluator.validate_params(params)


@pytest.mark.parametrize(
    "value", [0, -1, True, False, 1.5, 1.0, "1", 2147483648, float("inf"), float("nan"), [], {}]
)
def test_invalid_plan_leverage(value: JsonValue) -> None:
    """Never coerce booleans or floats to leverage or overflow storage.

    Given: manual-plan parameters containing invalid leverage.
    When: the evaluator validates the parameters directly.
    Then: validation raises ValueError without numeric coercion.
    """
    evaluator = ManualOnceEvaluator()
    with pytest.raises(ValueError):
        evaluator.validate_params({"order_type": "market", "side": "buy", "leverage": value})


def test_explicit_null_quantity_rejected() -> None:
    """Absent stored quantity remains distinct from an invalid supplied null.

    Given: manual-plan parameters with an explicitly null quantity.
    When: the evaluator validates the parameters.
    Then: validation rejects the supplied null quantity.
    """
    evaluator = ManualOnceEvaluator()
    with pytest.raises(ValueError):
        evaluator.validate_params({"order_type": "market", "side": "buy", "quantity": None})


@pytest.mark.parametrize("leverage", [None, 1, 2147483647])
def test_stored_plan_optional_numbers(leverage: int | None) -> None:
    """Stored plans omit quantity and may retain null optional amounts.

    Given: stored parameters omitting quantity with null prices and valid leverage.
    When: the evaluator validates the parameters.
    Then: validation succeeds without requiring quantity in the parameter mapping.
    """
    ManualOnceEvaluator().validate_params(
        {
            "order_type": "market",
            "side": "buy",
            "price": None,
            "stop_price": None,
            "leverage": leverage,
        }
    )


def test_finite_plan_amounts_accepted() -> None:
    """Integer and fractional positive amounts preserve valid plans.

    Given: a stop-limit plan with positive finite integer and fractional amounts.
    When: the evaluator validates the parameters.
    Then: validation accepts the plan.
    """
    ManualOnceEvaluator().validate_params(
        {"order_type": "stop_limit", "side": "buy", "quantity": 0.01, "price": 2, "stop_price": 1.5}
    )
