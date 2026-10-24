"""Exact xStocks unit conversions require scoped, effective and already-known evidence."""

from datetime import UTC
from datetime import datetime
from datetime import timedelta
from decimal import Decimal
from decimal import localcontext
from fractions import Fraction
from typing import cast

import pytest
from pydantic import ValidationError

from snapper.core.xstock_rebase import XStockInterpretation
from snapper.core.xstock_rebase import XStockMultiplierWitness
from snapper.core.xstock_rebase import XStockQuantityConvention
from snapper.core.xstock_rebase import XStockRebaseError
from snapper.core.xstock_rebase import XStockUnitScope
from snapper.core.xstock_rebase import XStockUnitValue
from snapper.core.xstock_rebase import convert_xstock_units
from snapper.core.xstock_rebase import exact_xstock_decimal
from snapper.core.xstock_rebase import require_xstock_multiplier
from snapper.core.xstock_rebase import xstock_multiplier_digest
from snapper.core.xstock_rebase import xstock_quote_notional

_T0 = datetime(2026, 9, 1, tzinfo=UTC)
_T1 = _T0 + timedelta(days=1)


def _scope(**updates: object) -> XStockUnitScope:
    """Build independently specified catalog and issuer identities for one market."""
    payload: dict[str, object] = {
        "exchange": "kraken",
        "instrument_public_id": "00000000-0000-4000-8000-000000000001",
        "spv_asset_id": "AAPLSPV",
        "underlying_asset_id": "US0378331005",
        "quote_asset_id": "USD",
        "network": "solana",
        "token_contract": "test-issuer-mint-AAPL",
    }
    return XStockUnitScope.model_validate(payload | updates)


def _witness(**updates: object) -> XStockMultiplierWitness:
    """Build a bounded observation without deriving its clocks from a wall schedule."""
    payload: dict[str, object] = {
        "public_id": "00000000-0000-4000-8000-000000000002",
        "scope": _scope(),
        "multiplier": "2",
        "source": "test-provider:multiplier-history",
        "source_record_id": "observed-multiplier-1",
        "source_receipt_digest": "a" * 64,
        "effective_from": _T0,
        "effective_until": _T1,
        "observed_at": _T0,
        "known_at": _T0,
    }
    return XStockMultiplierWitness.model_validate(payload | updates)


def _clocks(**updates: object) -> XStockInterpretation:
    """Keep economic time independent from the later interpretation clock."""
    payload: dict[str, object] = {
        "economic_at": _T0 + timedelta(hours=1),
        "interpreted_at": _T0 + timedelta(hours=2),
    }
    return XStockInterpretation.model_validate(payload | updates)


def _value(**updates: object) -> XStockUnitValue:
    """Provide explicit raw SPV quantity and its corresponding quote price."""
    payload: dict[str, object] = {
        "scope": _scope(),
        "quantity_convention": "spv_token",
        "price_convention": "quote_per_spv_token",
        "quantity": "3",
        "price": "100",
    }
    return XStockUnitValue.model_validate(payload | updates)


@pytest.mark.parametrize("quantity", ["3", "0", "-3", "0.76923076"])
def test_roundtrip_preserves_exact_quote_notional(quantity: str) -> None:
    """Corresponding price and quantity units conserve mathematical quote value.

    Given: An exact raw quotation and a multiplier effective for the observation.
    When: It is converted to equity units and back without quantization.
    Then: Both original operands and their quote notional are preserved exactly.
    """
    raw = _value(quantity=quantity)
    equity = convert_xstock_units(raw, _witness(), _clocks(), "underlying_equity")
    restored = convert_xstock_units(equity, _witness(), _clocks(), "spv_token")
    assert equity.quantity == Decimal(quantity) * 2
    assert equity.price == Decimal("50")
    assert restored == raw
    assert xstock_quote_notional(equity) == xstock_quote_notional(raw)


def test_real_raw_quantum_is_not_replaced_by_a_rounded_display() -> None:
    """Original raw evidence survives a finer-than-display equity representation.

    Given: A provider raw token quantity and multiplier whose product has nine decimals.
    When: The quotation is represented in equity units.
    Then: Its exact amount is retained instead of substituting an eight-decimal display.
    """
    raw = _value(quantity="0.76923076", price="260")
    equity = convert_xstock_units(raw, _witness(multiplier="1.3"), _clocks(), "underlying_equity")
    assert equity.quantity == Decimal("0.999999988")
    assert equity.price == Decimal("200")
    assert xstock_quote_notional(equity) == Decimal("199.999997600")


@pytest.mark.parametrize(
    ("value", "multiplier", "target"),
    [
        (
            {
                "quantity_convention": "underlying_equity",
                "price_convention": "quote_per_underlying_equity",
                "quantity": "0.99999999",
                "price": "200",
            },
            "1.3",
            "spv_token",
        ),
        ({"price": "1"}, "3", "underlying_equity"),
    ],
)
def test_nonterminating_conversion_requires_new_exact_evidence(
    value: dict[str, str], multiplier: str, target: XStockQuantityConvention
) -> None:
    """An approximate inverse must not invent exact ledger quantities or prices.

    Given: A reported quotation whose requested conversion is a recurring decimal.
    When: Exact normalization is requested without independent raw operands.
    Then: Conversion refuses and the original frozen quotation remains unchanged.
    """
    observed = _value(**value)
    original = observed.model_dump(mode="json")
    with pytest.raises(XStockRebaseError, match="nonterminating"):
        convert_xstock_units(observed, _witness(multiplier=multiplier), _clocks(), target)
    assert observed.model_dump(mode="json") == original


def test_conversion_is_independent_of_ambient_decimal_rounding() -> None:
    """Low caller precision cannot silently round a financial unit conversion.

    Given: Exact operands with substantially more digits than the active context.
    When: Conversion and notional evaluation run with precision two.
    Then: The returned operands and notional retain all mathematical digits.
    """
    raw = _value(quantity="123456789.12345678", price="125")
    with localcontext() as context:
        context.prec = 2
        equity = convert_xstock_units(
            raw, _witness(multiplier="1.25"), _clocks(), "underlying_equity"
        )
        notional = xstock_quote_notional(equity)
    assert equity.quantity == Decimal("154320986.404320975")
    assert equity.price == Decimal("100")
    assert notional == Decimal("15432098640.432097500")


@pytest.mark.parametrize("unit", ["spv_token", "underlying_equity"])
def test_identity_conversion_still_checks_available_evidence(
    unit: XStockQuantityConvention,
) -> None:
    """An unchanged representation does not bypass multiplier scope and clock proof.

    Given: A quotation already expressed in the requested target view.
    When: An identity conversion is requested using valid evidence.
    Then: Equal operands are returned only after the same witness checks.
    """
    value = _value(
        quantity_convention=unit,
        price_convention=(
            "quote_per_spv_token" if unit == "spv_token" else "quote_per_underlying_equity"
        ),
    )
    assert convert_xstock_units(value, _witness(), _clocks(), unit) == value
    with pytest.raises(XStockRebaseError, match="not_yet_known"):
        convert_xstock_units(value, _witness(known_at=_T1), _clocks(), unit)


@pytest.mark.parametrize(
    ("field", "other"),
    [
        ("exchange", "paper"),
        ("instrument_public_id", "00000000-0000-4000-8000-000000000099"),
        ("spv_asset_id", "MSFTSPV"),
        ("underlying_asset_id", "other-equity"),
        ("quote_asset_id", "EUR"),
        ("network", "ethereum"),
        ("token_contract", "other-mint"),
    ],
)
def test_multiplier_cannot_cross_an_asset_or_network_scope(field: str, other: str) -> None:
    """A matching display ticker cannot substitute for exact source identity.

    Given: A witness differing in one market, asset, network or token identity.
    When: It is applied to the original quotation.
    Then: Normalization refuses before producing any monetary operand.
    """
    with pytest.raises(XStockRebaseError, match="scope_mismatch"):
        convert_xstock_units(
            _value(), _witness(scope=_scope(**{field: other})), _clocks(), "underlying_equity"
        )


@pytest.mark.parametrize("economic_at", [_T0 - timedelta(microseconds=1), _T1])
def test_multiplier_effectivity_uses_a_half_open_interval(economic_at: datetime) -> None:
    """A pending or expired multiplier cannot reinterpret an observation outside its interval.

    Given: A witnessed interval with explicit lower and upper effective bounds.
    When: Economic time lies just before it or exactly at its upper bound.
    Then: Interpretation refuses independently of when the record was fetched.
    """
    with pytest.raises(XStockRebaseError, match="not_effective"):
        require_xstock_multiplier(
            _witness(), _scope(), _clocks(economic_at=economic_at, interpreted_at=_T1)
        )


@pytest.mark.parametrize(
    "updates",
    [{"effective_until": _T0}, {"observed_at": _T1}, {"known_at": _T0 - timedelta(microseconds=1)}],
)
def test_invalid_multiplier_intervals_and_knowledge_are_rejected(
    updates: dict[str, datetime],
) -> None:
    """Malformed clocks cannot become valid merely because a multiplier is positive.

    Given: An empty interval or local observation later than durable knowledge.
    When: The immutable witness is constructed.
    Then: The inconsistent provenance is rejected.
    """
    with pytest.raises(ValidationError):
        _witness(**updates)


def test_future_economic_time_is_not_a_completed_interpretation() -> None:
    """An interpretation clock cannot precede the fact being interpreted.

    Given: An economic timestamp after the proposed interpretation.
    When: Its explicit clock context is constructed.
    Then: The future observation refuses before any conversion.
    """
    with pytest.raises(ValidationError, match="after interpretation"):
        _clocks(economic_at=_T1)


@pytest.mark.parametrize("field", ["quantity", "price"])
@pytest.mark.parametrize(
    "invalid", [True, 1, 1.0, "NaN", "Infinity", "-Infinity", "bad", "1e257", "1e-257", "9" * 257]
)
def test_quotation_operands_require_bounded_exact_decimal_evidence(
    field: str, invalid: object
) -> None:
    """Compatibility floats and unbounded or nonfinite text cannot enter unit arithmetic.

    Given: A non-exact, malformed or excessively large quotation operand.
    When: It is supplied to the immutable quotation contract.
    Then: Validation refuses rather than rounding or allocating unbounded integers.
    """
    with pytest.raises(ValidationError):
        _value(**{field: invalid})


@pytest.mark.parametrize("invalid", [True, 1, 1.0, "0", "-1", "NaN", "1e257", "9" * 257])
def test_multiplier_requires_positive_bounded_exact_evidence(invalid: object) -> None:
    """A multiplier is an observed positive ratio rather than a coercible number.

    Given: A non-positive, inexact or unbounded multiplier input.
    When: The provider witness is constructed.
    Then: Validation refuses the proposed evidence.
    """
    with pytest.raises(ValidationError):
        _witness(multiplier=invalid)


@pytest.mark.parametrize("convention", ["spv_token", "underlying_equity"])
def test_mixed_price_and_quantity_units_are_rejected(convention: XStockQuantityConvention) -> None:
    """A source profile must identify corresponding dimensions for both operands.

    Given: A quantity whose price label belongs to the opposite representation.
    When: The quotation is constructed.
    Then: Its apparently plausible numeric product is not accepted.
    """
    opposite = "quote_per_underlying_equity" if convention == "spv_token" else "quote_per_spv_token"
    with pytest.raises(ValidationError, match="do not agree"):
        _value(quantity_convention=convention, price_convention=opposite)


def test_unchecked_model_updates_are_revalidated_by_consumers() -> None:
    """Frozen model-copy construction is not a financial validation bypass.

    Given: An unchecked copy replacing an exact operand or multiplier with a float.
    When: A public conversion or digest consumer receives it.
    Then: The proposed interpretation rejects that altered contract.
    """
    with pytest.raises(ValidationError):
        xstock_quote_notional(_value().model_copy(update={"quantity": 1.0}))
    with pytest.raises(ValidationError):
        xstock_multiplier_digest(_witness().model_copy(update={"multiplier": 2.0}))


def test_unknown_target_unit_refuses_even_for_valid_witnesses() -> None:
    """An internal caller cannot request an unlabeled representation.

    Given: An otherwise valid exact quotation and multiplier witness.
    When: An unsupported target label reaches the public conversion boundary.
    Then: Conversion refuses rather than treating it as SPV by fallback.
    """
    with pytest.raises(XStockRebaseError, match="target_unit_unsupported"):
        convert_xstock_units(
            _value(), _witness(), _clocks(), cast(XStockQuantityConvention, "unknown")
        )


@pytest.mark.parametrize(
    ("fraction", "expected"),
    [
        (Fraction(0), "0"),
        (Fraction(-1, 8), "-0.125"),
        (Fraction(7, 25), "0.28"),
        (Fraction(3, 20), "0.15"),
        (Fraction(42), "42"),
    ],
)
def test_finite_internal_ratios_have_context_independent_decimal_encodings(
    fraction: Fraction, expected: str
) -> None:
    """Exact internal ratios preserve their sign and every terminating digit.

    Given: Rational results with integer, binary or decimal denominator factors.
    When: They are encoded without ambient Decimal division.
    Then: Their mathematical Decimal values remain exact.
    """
    assert exact_xstock_decimal(fraction) == Decimal(expected)


def test_multiplier_digest_canonicalizes_values_without_losing_source_identity() -> None:
    """Equivalent decimal spellings hash equally while changed provenance remains visible.

    Given: Equivalent multiplier values with the same provider record and clocks.
    When: Their immutable witness digests are computed.
    Then: Spelling does not split identity and a different receipt changes the digest.
    """
    assert xstock_multiplier_digest(_witness(multiplier="2.00")) == xstock_multiplier_digest(
        _witness(multiplier=Decimal("2"))
    )
    assert xstock_multiplier_digest(_witness()) != xstock_multiplier_digest(
        _witness(source_receipt_digest="b" * 64)
    )


@pytest.mark.parametrize("quantity", ["1e256", "1e-256"])
def test_valid_exponent_boundaries_do_not_fail_because_of_derived_zero_padding(
    quantity: str,
) -> None:
    """Canonical derived coefficients preserve values already inside the operand bounds.

    Given: A one-digit raw operand at either supported exponent boundary.
    When: Exact conversion and its inverse produce mathematically valid operands.
    Then: Integer zero padding does not turn the accepted value into an oversized coefficient.
    """
    value = _value(quantity=quantity)
    converted = convert_xstock_units(value, _witness(), _clocks(), "underlying_equity")
    assert converted.quantity == Decimal(quantity) * 2
    assert convert_xstock_units(converted, _witness(), _clocks(), "spv_token") == value
