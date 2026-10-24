"""Golden contract-position folds, validation and Decimal-context regressions.

The arithmetic and harmonic entry conventions are documented by the exchange
at https://www.bybit.com/en/help-center/article/Average-entry-price. Kraken's
linear and reciprocal P&L formulas support the settlement examples below:
https://support.kraken.com/articles/360022835511-calculation-of-profit-and-loss-derivatives.
These are generic normalized contract units, not venue-order eligibility tests.
"""

from dataclasses import FrozenInstanceError
from decimal import ROUND_DOWN
from decimal import ROUND_UP
from decimal import Decimal
from decimal import Inexact
from decimal import Overflow
from decimal import localcontext
from typing import cast

import pytest

from snapper.core.contract_position import ContractPosition
from snapper.core.contract_position import apply_contract_fill
from snapper.core.contract_valuation import ContractValuation
from snapper.core.contract_valuation import InverseContractValuation
from snapper.core.contract_valuation import LinearContractValuation

_LINEAR = LinearContractValuation(Decimal("0.5"))
_INVERSE = InverseContractValuation(Decimal("100"))
_MODELS: tuple[ContractValuation, ...] = (_LINEAR, _INVERSE)
_INVALID: tuple[object, ...] = (
    None,
    True,
    False,
    1,
    1.0,
    "1",
    Decimal("NaN"),
    Decimal("sNaN"),
    Decimal("Infinity"),
    Decimal("-Infinity"),
)
_INVALID_POSITIVE = (*_INVALID, Decimal(0), Decimal("-1"))
_UNTYPED_MODELS: tuple[object, ...] = (None, True, "linear", {"multiplier": Decimal(1)})


@pytest.mark.parametrize("model", _UNTYPED_MODELS)
def test_untyped_specification_is_rejected_even_for_flat_or_noop(model: object) -> None:
    """Require explicit immutable contract models.

    Given: Untyped payoff metadata.
    When: A position or zero-fill fold is requested.
    Then: The metadata is refused before arithmetic.
    """
    invalid = cast(ContractValuation, model)
    with pytest.raises(ValueError, match="explicit contract"):
        ContractPosition(invalid, Decimal(0), None)
    position = ContractPosition(_LINEAR, Decimal(0), None)
    with pytest.raises(ValueError, match="explicit contract"):
        apply_contract_fill(invalid, position, Decimal(0), Decimal(100))


@pytest.mark.parametrize("model", _MODELS)
@pytest.mark.parametrize("quantity", _INVALID)
def test_position_and_fill_quantities_require_finite_decimals(
    model: ContractValuation, quantity: object
) -> None:
    """Reject invalid quantities without coercion.

    Given: A non-Decimal or non-finite quantity.
    When: A position is constructed or a fill is applied.
    Then: Both paths reject the operand.
    """
    invalid = cast(Decimal, quantity)
    with pytest.raises(ValueError, match="finite Decimal"):
        ContractPosition(model, invalid, Decimal(100))
    position = ContractPosition(model, Decimal(0), None)
    with pytest.raises(ValueError, match="finite Decimal"):
        apply_contract_fill(model, position, invalid, Decimal(100))


@pytest.mark.parametrize("model", _MODELS)
@pytest.mark.parametrize("price", _INVALID_POSITIVE)
def test_nonflat_entry_and_all_fill_prices_require_positive_finite_decimals(
    model: ContractValuation, price: object
) -> None:
    """Validate prices before all fill paths.

    Given: An invalid entry or execution price.
    When: A non-flat position or zero or nonzero fill is requested.
    Then: Invalid prices fail before a no-op can return.
    """
    invalid = cast(Decimal, price)
    with pytest.raises(ValueError):
        ContractPosition(model, Decimal(1), invalid)
    position = ContractPosition(model, Decimal(0), None)
    for quantity in (Decimal(0), Decimal(1)):
        with pytest.raises(ValueError):
            apply_contract_fill(model, position, quantity, invalid)


@pytest.mark.parametrize("entry", (Decimal(0), Decimal(100), "100"))
def test_flat_position_requires_an_absent_entry_basis(entry: object) -> None:
    """Keep flat entry basis absent.

    Given: Zero contracts and a retained entry value.
    When: The position record is constructed.
    Then: Its inconsistent basis is rejected.
    """
    with pytest.raises(ValueError, match="flat contract position"):
        ContractPosition(_LINEAR, Decimal(0), cast(Decimal, entry))


@pytest.mark.parametrize("position", (None, True, "flat", Decimal(0)))
def test_fold_requires_a_typed_position_even_for_zero_fill(position: object) -> None:
    """Reject untyped position records.

    Given: An object that is not a ContractPosition.
    When: A zero fill is requested.
    Then: The fold refuses it before returning a no-op.
    """
    with pytest.raises(ValueError, match="must be a ContractPosition"):
        apply_contract_fill(_LINEAR, cast(ContractPosition, position), Decimal(0), Decimal(100))


@pytest.mark.parametrize(
    "other_model",
    (LinearContractValuation(Decimal(1)), InverseContractValuation(Decimal("0.5"))),
)
@pytest.mark.parametrize("fill_quantity", (Decimal(0), Decimal(1)))
def test_changed_multiplier_or_payoff_cannot_be_averaged_into_existing_pool(
    other_model: ContractValuation, fill_quantity: Decimal
) -> None:
    """Prevent averaging across changed contract specifications.

    Given: A position with a different multiplier or payoff model.
    When: A zero or increasing fill uses the changed specification.
    Then: The entire fold is refused.
    """
    position = ContractPosition(_LINEAR, Decimal(1), Decimal(100))
    with pytest.raises(ValueError, match="specification changed"):
        apply_contract_fill(other_model, position, fill_quantity, Decimal(200))


@pytest.mark.parametrize("model", _MODELS)
@pytest.mark.parametrize("old_quantity", (Decimal(0), Decimal("-0"), Decimal(3), Decimal(-3)))
def test_zero_fill_retains_original_position_after_validation(
    model: ContractValuation, old_quantity: Decimal
) -> None:
    """Preserve unchanged positions for signed zero fills.

    Given: A validated flat, long or short pool.
    When: A negative-zero fill arrives at a valid price.
    Then: Position identity and all zero outcome quantities are preserved.
    """
    entry = None if old_quantity.is_zero() else Decimal(200)
    position = ContractPosition(model, old_quantity, entry)
    outcome = apply_contract_fill(model, position, Decimal("-0"), Decimal(400))
    assert outcome.position is position
    assert outcome.realized_settlement_pnl == 0
    assert outcome.closed_quantity == 0
    assert outcome.opened_quantity == 0


@pytest.mark.parametrize("model", _MODELS)
@pytest.mark.parametrize("quantity", (Decimal(3), Decimal(-3), Decimal("1e-30"), Decimal("-1e-30")))
def test_opening_flat_preserves_exact_contract_quantity_without_an_epsilon(
    model: ContractValuation, quantity: Decimal
) -> None:
    """Open exact signed quantities without an epsilon.

    Given: A flat position under either model.
    When: A long, short or tiny fill arrives.
    Then: The fill opens at its execution price without mutating the input.
    """
    position = ContractPosition(model, Decimal(0), None)
    outcome = apply_contract_fill(model, position, quantity, Decimal(200))
    assert outcome.position == ContractPosition(model, quantity, Decimal(200))
    assert outcome.realized_settlement_pnl == 0
    assert outcome.closed_quantity == 0
    assert outcome.opened_quantity == quantity.copy_abs()
    assert position.quantity == 0
    assert position.entry_price is None


@pytest.mark.parametrize("sign", (Decimal(1), Decimal(-1)))
@pytest.mark.parametrize(("model", "expected_entry"), ((_LINEAR, "300"), (_INVERSE, "200")))
def test_same_side_entries_use_independent_arithmetic_or_harmonic_goldens(
    sign: Decimal, model: ContractValuation, expected_entry: str
) -> None:
    """Use independent arithmetic and harmonic entry goldens.

    Given: One contract at 100 on either side.
    When: Two more contracts are added at 400.
    Then: Linear entry is 300 and inverse entry is 200 with no realized P&L.
    """
    position = ContractPosition(model, sign, Decimal(100))
    outcome = apply_contract_fill(model, position, sign * 2, Decimal(400))
    assert outcome.position.quantity == sign * 3
    assert outcome.position.entry_price == Decimal(expected_entry)
    assert outcome.position.specification is model
    assert outcome.realized_settlement_pnl == 0
    assert outcome.closed_quantity == 0
    assert outcome.opened_quantity == 2
    assert position.entry_price == 100


def test_official_inverse_entry_example_has_twelve_thousand_basis() -> None:
    """Reproduce the official inverse average-entry example.

    Given: Fifty inverse contracts entered at 10000.
    When: Fifty more enter at 15000.
    Then: One hundred contracts have a 12000 entry basis.
    """
    position = ContractPosition(_INVERSE, Decimal(50), Decimal(10000))
    outcome = apply_contract_fill(_INVERSE, position, Decimal(50), Decimal(15000))
    assert outcome.position.quantity == 100
    assert outcome.position.entry_price == 12000


@pytest.mark.parametrize(
    ("model", "old_quantity", "fill_quantity", "price", "expected_pnl"),
    (
        (_LINEAR, "3", "-1", "400", "100"),
        (_INVERSE, "3", "-1", "400", "0.25"),
        (_LINEAR, "-3", "1", "100", "50"),
        (_INVERSE, "-3", "1", "100", "0.5"),
        (_LINEAR, "3", "-1", "100", "-50"),
        (_INVERSE, "3", "-1", "100", "-0.5"),
        (_LINEAR, "-3", "1", "400", "-100"),
        (_INVERSE, "-3", "1", "400", "-0.25"),
        (_LINEAR, "3", "-1", "200", "0"),
        (_INVERSE, "3", "-1", "200", "0"),
    ),
)
def test_partial_closes_keep_basis_and_realize_only_closed_quantity(
    model: ContractValuation,
    old_quantity: str,
    fill_quantity: str,
    price: str,
    expected_pnl: str,
) -> None:
    """Realize only the closed portion of a partial close.

    Given: Three long or short contracts entered at 200.
    When: One contract closes profitably, at a loss or at unchanged price.
    Then: Two retain their entry basis and only the closed unit realizes P&L.
    """
    position = ContractPosition(model, Decimal(old_quantity), Decimal(200))
    outcome = apply_contract_fill(model, position, Decimal(fill_quantity), Decimal(price))
    expected_quantity = Decimal(2) if Decimal(old_quantity) > 0 else Decimal(-2)
    assert outcome.position == ContractPosition(model, expected_quantity, Decimal(200))
    assert outcome.realized_settlement_pnl == Decimal(expected_pnl)
    assert outcome.closed_quantity == 1
    assert outcome.opened_quantity == 0


@pytest.mark.parametrize(
    ("model", "quantity", "expected_pnl"),
    (
        (_LINEAR, "3", "300"),
        (_INVERSE, "3", "0.75"),
        (_LINEAR, "-3", "-300"),
        (_INVERSE, "-3", "-0.75"),
    ),
)
def test_full_close_clears_entry_and_realizes_entire_old_pool(
    model: ContractValuation, quantity: str, expected_pnl: str
) -> None:
    """Clear the entry basis on a full close.

    Given: Three long or short contracts entered at 200.
    When: An exact opposite fill closes them at 400.
    Then: The outcome is flat with the literal full settlement payoff.
    """
    old_quantity = Decimal(quantity)
    position = ContractPosition(model, old_quantity, Decimal(200))
    outcome = apply_contract_fill(model, position, old_quantity.copy_negate(), Decimal(400))
    assert outcome.position == ContractPosition(model, Decimal(0), None)
    assert outcome.realized_settlement_pnl == Decimal(expected_pnl)
    assert outcome.closed_quantity == 3
    assert outcome.opened_quantity == 0


@pytest.mark.parametrize(
    ("model", "old_quantity", "fill_quantity", "price", "expected"),
    (
        (_LINEAR, "3", "-5", "400", ("-2", "300")),
        (_INVERSE, "3", "-5", "400", ("-2", "0.75")),
        (_LINEAR, "-3", "5", "100", ("2", "150")),
        (_INVERSE, "-3", "5", "100", ("2", "1.5")),
    ),
)
def test_flip_realizes_old_side_and_opens_only_residual_at_fill_price(
    model: ContractValuation,
    old_quantity: str,
    fill_quantity: str,
    price: str,
    expected: tuple[str, str],
) -> None:
    """Separate closing and opening quantities on a flip.

    Given: A three-contract long or short position.
    When: An opposite five-contract fill crosses through flat.
    Then: Only three realize old-side P&L and two open at the new fill price.
    """
    position = ContractPosition(model, Decimal(old_quantity), Decimal(200))
    outcome = apply_contract_fill(model, position, Decimal(fill_quantity), Decimal(price))
    expected_quantity, expected_pnl = expected
    assert outcome.position == ContractPosition(model, Decimal(expected_quantity), Decimal(price))
    assert outcome.realized_settlement_pnl == Decimal(expected_pnl)
    assert outcome.closed_quantity == 3
    assert outcome.opened_quantity == 2


@pytest.mark.parametrize(("model", "expected_net"), ((_LINEAR, "-150"), (_INVERSE, "-0.75")))
def test_scaled_entry_and_split_closes_conserve_independent_total_payoff(
    model: ContractValuation, expected_net: str
) -> None:
    """Conserve total settlement payoff through scaling and split closes.

    Given: An initially flat position under either contract model.
    When: The fixed sequence opens one at 100 and two at 400 then closes at 400 and 100.
    Then: The literal net result is minus 150 quote or minus 0.75 base units.
    """
    position = ContractPosition(model, Decimal(0), None)
    realized = Decimal(0)
    for quantity, price in ((1, 100), (2, 400), (-1, 400), (-2, 100)):
        outcome = apply_contract_fill(model, position, Decimal(quantity), Decimal(price))
        position = outcome.position
        realized += outcome.realized_settlement_pnl
    assert position == ContractPosition(model, Decimal(0), None)
    assert realized == Decimal(expected_net)


def test_position_and_outcome_records_remain_immutable() -> None:
    """Keep position and fill outcomes immutable.

    Given: A successful fill and its original position.
    When: Each record field is assigned another value.
    Then: Frozen dataclasses reject every mutation.
    """
    position = ContractPosition(_LINEAR, Decimal(1), Decimal(100))
    outcome = apply_contract_fill(_LINEAR, position, Decimal(1), Decimal(200))
    for record, name, value in (
        (position, "quantity", Decimal(9)),
        (position, "entry_price", Decimal(9)),
        (position, "specification", _INVERSE),
        (outcome, "position", position),
        (outcome, "realized_settlement_pnl", Decimal(9)),
        (outcome, "closed_quantity", Decimal(9)),
        (outcome, "opened_quantity", Decimal(9)),
    ):
        with pytest.raises(FrozenInstanceError):
            setattr(record, name, value)


@pytest.mark.parametrize("fill_quantity", (Decimal(1), Decimal(-1)))
def test_quantity_precision_loss_is_refused_and_caller_context_is_unchanged(
    fill_quantity: Decimal,
) -> None:
    """Require exact quantity conservation.

    Given: A large pool under insufficient Decimal precision.
    When: A one-contract increase or decrease would be rounded away.
    Then: The fold fails and leaves both pool and caller context unchanged.
    """
    position = ContractPosition(_LINEAR, Decimal("1e10"), Decimal(100))
    with localcontext() as context:
        context.prec = 5
        context.traps[Inexact] = False
        context.clear_flags()
        with pytest.raises(ValueError, match="arithmetic failed"):
            apply_contract_fill(_LINEAR, position, fill_quantity, Decimal(100))
        assert context.traps[Inexact] is False
        assert context.flags[Inexact] is False
    assert position.quantity == Decimal("1e10")


@pytest.mark.parametrize("model", _MODELS)
def test_trapped_entry_rounding_is_refused_without_mutating_position(
    model: ContractValuation,
) -> None:
    """Honor trapped entry arithmetic.

    Given: An existing position and an enabled Inexact trap.
    When: A same-side fill requires an inexact weighted entry.
    Then: The whole fold is refused and the original basis remains intact.
    """
    position = ContractPosition(model, Decimal(1), Decimal(100))
    with localcontext() as context:
        context.prec = 3
        context.traps[Inexact] = True
        with pytest.raises(ValueError, match="arithmetic failed"):
            apply_contract_fill(model, position, Decimal(2), Decimal(300))
    assert position.entry_price == 100


@pytest.mark.parametrize(
    ("model", "rounding", "expected"),
    (
        (_LINEAR, ROUND_DOWN, "100"),
        (_LINEAR, ROUND_UP, "101"),
        (_INVERSE, ROUND_DOWN, "133"),
        (_INVERSE, ROUND_UP, "134"),
    ),
)
def test_entry_calculation_honors_caller_rounding_mode(
    model: ContractValuation, rounding: str, expected: str
) -> None:
    """Honor the caller's entry rounding mode.

    Given: A three-digit Decimal context with upward or downward rounding.
    When: A same-side fill produces a nonrepresentable exact entry.
    Then: The resulting entry matches the independent directional golden value.
    """
    position = ContractPosition(model, Decimal(1), Decimal(100))
    price = Decimal(101) if isinstance(model, LinearContractValuation) else Decimal(200)
    with localcontext() as context:
        context.prec = 3
        context.rounding = rounding
        outcome = apply_contract_fill(model, position, Decimal(1), price)
    assert outcome.position.quantity == 2
    assert outcome.position.entry_price == Decimal(expected)


@pytest.mark.parametrize("model", _MODELS)
def test_untrapped_nonfinite_entry_arithmetic_is_refused(model: ContractValuation) -> None:
    """Reject untrapped non-finite entry arithmetic.

    Given: A narrow exponent range with overflow traps disabled.
    When: A weighted entry calculation overflows.
    Then: The fold refuses an infinite entry instead of creating a position.
    """
    position = ContractPosition(model, Decimal(1), Decimal("1e5"))
    with localcontext() as context:
        context.prec = 3
        context.Emax = 4
        context.traps[Overflow] = False
        with pytest.raises(ValueError, match="non-finite Decimal"):
            apply_contract_fill(model, position, Decimal(1), Decimal("1e5"))


@pytest.mark.parametrize("model", _MODELS)
def test_underflow_cannot_turn_positive_entry_basis_into_zero(model: ContractValuation) -> None:
    """Reject entry bases rounded down to zero.

    Given: An entry below the Decimal context's representable exponent range.
    When: A same-side fill recomputes the entry.
    Then: A zero result is refused as an invalid positive basis.
    """
    position = ContractPosition(model, Decimal(1), Decimal("1e-10"))
    with localcontext() as context:
        context.prec = 3
        context.Emax = 4
        context.Emin = -4
        context.traps[Overflow] = False
        with pytest.raises(ValueError, match="positive"):
            apply_contract_fill(model, position, Decimal(1), Decimal("1e-10"))


@pytest.mark.parametrize("model", _MODELS)
def test_closing_payoff_traps_propagate_without_a_position_update(model: ContractValuation) -> None:
    """Propagate settlement arithmetic refusals atomically.

    Given: A position and an Inexact-trapping context.
    When: A close requires inexact settlement payoff arithmetic.
    Then: The fold raises and the original quantity and entry remain unchanged.
    """
    position = ContractPosition(model, Decimal(3), Decimal(200))
    with localcontext() as context:
        context.prec = 3
        context.traps[Inexact] = True
        with pytest.raises(ValueError, match="valuation failed"):
            apply_contract_fill(model, position, Decimal(-1), Decimal("300.1"))
    assert position.quantity == 3
    assert position.entry_price == 200
