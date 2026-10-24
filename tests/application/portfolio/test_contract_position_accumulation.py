"""Modeled inverse P&L counters must retain small later realizations exactly."""

from decimal import Decimal
from decimal import localcontext
from typing import cast

import pytest

from snapper.application.portfolio.contract_economics import ContractEconomicsError
from snapper.application.portfolio.contract_position_ledger import ContractPositionValue
from snapper.application.portfolio.contract_position_ledger import PositionDecimalPolicy
from snapper.application.portfolio.contract_position_ledger import fold_position_source
from tests.application.portfolio.test_contract_position_ledger import _source
from tests.application.portfolio.test_contract_position_ledger import _unit
from tests.application.portfolio.test_contract_position_ledger import _value


def test_a_small_inverse_realization_accumulates_after_a_larger_partial_close() -> None:
    """Preserve existing modeled trade deltas without requiring an account currency rounding guess.

    Given: An inverse position first closes 1000 USD1 contracts after its price doubles.
    When: One more contract closes one price tick above its original entry.
    Then: Both modeled realizations survive in the cumulative counter without extra rounding.
    """
    first_unit = _unit("-1000", "20000", inverse=True)
    _, first = fold_position_source(
        _value("1002", "10000"),
        first_unit,
        _source(first_unit, ("1002", "10000"), ("2", "10000")),
    )
    assert first.realized_pnl_since_anchor == Decimal("0.05")
    second_unit = _unit("-1", "10000.5", inverse=True)
    _, second = fold_position_source(
        first,
        second_unit,
        _source(second_unit, ("2", "10000"), ("1", "10000")),
        first_unit,
    )
    assert Decimal("0.05") < second.realized_pnl_since_anchor < Decimal("0.05000001")
    assert second.quantity == Decimal(1)
    assert second.quote_turnover_since_anchor == Decimal(1001)
    exponent = second.realized_pnl_since_anchor.as_tuple().exponent
    assert isinstance(exponent, int)
    assert exponent < -50


def test_historical_v1_arithmetic_keeps_its_original_exact_sum_refusal() -> None:
    """Preserve the identity and interpretation of the original recorded arithmetic policy.

    Given: The same two inverse partial closes are explicitly evaluated under historical v1.
    When: The later small delta cannot fit the original fixed50-digit cumulative context.
    Then: The historical policy retains its original refusal instead of changing meaning silently.
    """
    first_unit = _unit("-1000", "20000", inverse=True)
    _, first = fold_position_source(
        _value("1002", "10000"),
        first_unit,
        _source(first_unit, ("1002", "10000"), ("2", "10000")),
        decimal_policy="contract_context_50_half_even_v1",
    )
    second_unit = _unit("-1", "10000.5", inverse=True)
    with pytest.raises(ContractEconomicsError, match="economic_accumulation_unavailable"):
        fold_position_source(
            first,
            second_unit,
            _source(second_unit, ("2", "10000"), ("1", "10000")),
            first_unit,
            decimal_policy="contract_context_50_half_even_v1",
        )


def test_a_later_opening_preserves_long_accumulated_pnl_under_a_small_ambient_context() -> None:
    """Keep previously modeled exact totals when a later fill realizes no additional P&L.

    Given: A retained cumulative inverse P&L has more than50 significant decimal digits.
    When: Another opening fill is folded while the caller uses precision2.
    Then: The complete existing total survives while modeled position arithmetic uses its own policy.
    """
    total = Decimal("0.0500000049997500124993750312484375781210939453027350001")
    assert len(total.as_tuple().digits) > 50
    before = ContractPositionValue(
        quantity=Decimal(1),
        entry_price=Decimal(10000),
        realized_pnl_since_anchor=total,
        quote_turnover_since_anchor=Decimal(1001),
    )
    unit = _unit("1", "10000.5", inverse=True)
    source = _source(unit, ("1", "10000"), ("2", "10000.25"))
    with localcontext() as context:
        context.prec = 2
        _, after = fold_position_source(before, unit, source)
    assert after.realized_pnl_since_anchor == total
    assert after.quote_turnover_since_anchor == Decimal(1002)
    assert after.quantity == Decimal(2)


def test_unknown_position_arithmetic_policy_refuses_before_financial_modeling() -> None:
    """Require an explicit supported interpretation instead of guessing an unknown policy.

    Given: A structurally valid position and native source transition.
    When: A runtime caller supplies an unsupported arithmetic policy identifier.
    Then: The fold refuses without selecting a default interpretation.
    """
    unit = _unit()
    with pytest.raises(ValueError, match="policy unsupported"):
        fold_position_source(
            _value("0", None),
            unit,
            _source(unit, ("0", None), ("1", "100")),
            decimal_policy=cast(PositionDecimalPolicy, "future_policy"),
        )


def test_exact_counter_overflow_remains_an_explicit_financial_refusal() -> None:
    """Keep the established numeric exponent boundary while preserving representable exact totals.

    Given: A retained modeled total is near the supported exponent limit.
    When: A genuine closing delta would carry that total beyond the supported range.
    Then: The fold refuses instead of persisting infinity or a rounded financial counter.
    """
    before = ContractPositionValue(
        quantity=Decimal(1),
        entry_price=Decimal(1),
        realized_pnl_since_anchor=Decimal("9e999999"),
        quote_turnover_since_anchor=Decimal(0),
    )
    unit = _unit("-1", "1e999999")
    with pytest.raises(ValueError, match="cumulative arithmetic unavailable"):
        fold_position_source(before, unit, _source(unit, ("1", "1"), ("0", None)))
