"""Convert explicitly witnessed xStocks units without inventing venue semantics.

Kraken SPV-token reporting and underlying-equity reporting are distinct units.
These models preserve the provider identity, interval and knowledge clock of a
supplied multiplier. They neither authenticate that evidence nor establish an
undocumented order-entry or market-data convention. Exact conversion refuses a
rounded displayed quantity that cannot establish finite raw token operands.
"""

import hashlib
import json
from decimal import Decimal
from fractions import Fraction
from typing import Annotated
from typing import Literal
from typing import Self
from typing import cast

from pydantic import AfterValidator
from pydantic import BaseModel
from pydantic import ConfigDict
from pydantic import model_validator

from snapper.core.execution_unit_snapshot import EvidenceDecimal
from snapper.core.execution_unit_snapshot import EvidenceDigest
from snapper.core.execution_unit_snapshot import EvidenceExchange
from snapper.core.execution_unit_snapshot import EvidenceInstant
from snapper.core.execution_unit_snapshot import EvidenceText
from snapper.core.execution_unit_snapshot import EvidenceUUID
from snapper.core.execution_unit_snapshot import PositiveEvidenceDecimal

XSTOCK_MULTIPLIER_HASH_DOMAIN = b"snapper:xstock-multiplier:v1\x00"
_MAX_OPERAND_DIGITS = 256
_MAX_OPERAND_EXPONENT = 256


class XStockRebaseError(ValueError):
    """Evidence or exact arithmetic cannot establish the requested unit conversion."""


def _bounded_operand(value: Decimal) -> Decimal:
    """Bound exact rational work before constructing large integer operands."""
    _, digits, raw_exponent = value.as_tuple()
    exponent = cast(int, raw_exponent)
    if len(digits) > _MAX_OPERAND_DIGITS or abs(exponent) > _MAX_OPERAND_EXPONENT:
        raise ValueError("xStocks decimal evidence exceeds the supported arithmetic bounds")
    return value


type XStockDecimal = Annotated[EvidenceDecimal, AfterValidator(_bounded_operand)]
type PositiveXStockDecimal = Annotated[PositiveEvidenceDecimal, AfterValidator(_bounded_operand)]
type XStockQuantityConvention = Literal["spv_token", "underlying_equity"]
type XStockPriceConvention = Literal["quote_per_spv_token", "quote_per_underlying_equity"]


class _ImmutableEvidence(BaseModel):
    """Revalidate even unchecked copies before accepting immutable financial evidence."""

    model_config = ConfigDict(frozen=True, extra="forbid", revalidate_instances="always")


class XStockUnitScope(_ImmutableEvidence):
    """Exact catalog and asset identities, never inferred from a display ticker."""

    exchange: EvidenceExchange
    instrument_public_id: EvidenceUUID
    spv_asset_id: EvidenceText
    underlying_asset_id: EvidenceText
    quote_asset_id: EvidenceText
    network: EvidenceText
    token_contract: EvidenceText


class XStockMultiplierWitness(_ImmutableEvidence):
    """One supplied provider observation with an explicit bounded effective interval.

    A producer must prove how the interval relates to the venue asset and retain
    the referenced receipt. A pending multiplier may be recorded before it takes
    effect, but a consumer cannot use it before its effective instant.
    """

    public_id: EvidenceUUID
    scope: XStockUnitScope
    multiplier: PositiveXStockDecimal
    source: EvidenceText
    source_record_id: EvidenceText
    source_receipt_digest: EvidenceDigest
    effective_from: EvidenceInstant
    effective_until: EvidenceInstant
    observed_at: EvidenceInstant
    known_at: EvidenceInstant

    @model_validator(mode="after")
    def validate_clocks(self) -> Self:
        """Refuse an empty interval or evidence known before its local observation.

        Returns:
            The same explicit observation without deriving an activation schedule.
        """
        if self.effective_from >= self.effective_until:
            raise ValueError("xStocks multiplier interval must be nonempty")
        if self.observed_at > self.known_at:
            raise ValueError("xStocks multiplier cannot be known before observation")
        return self


class XStockInterpretation(_ImmutableEvidence):
    """Economic observation time and the later clock at which evidence is interpreted."""

    economic_at: EvidenceInstant
    interpreted_at: EvidenceInstant

    @model_validator(mode="after")
    def validate_order(self) -> Self:
        """Refuse future economic observations in an already claimed interpretation.

        Returns:
            The same aware pair of independently meaningful clocks.
        """
        if self.economic_at > self.interpreted_at:
            raise ValueError("xStocks economic observation is after interpretation")
        return self


class XStockUnitValue(_ImmutableEvidence):
    """Quantity and corresponding quote price in one explicitly identified unit view.

    This is a mathematical quotation. Its product is not a substitute for a
    provider's independently recorded quote cost, fee or actual cash balance.
    """

    scope: XStockUnitScope
    quantity_convention: XStockQuantityConvention
    price_convention: XStockPriceConvention
    quantity: XStockDecimal
    price: PositiveXStockDecimal

    @model_validator(mode="after")
    def validate_dimensions(self) -> Self:
        """Prevent a raw quantity from being valued with an equity-unit price.

        Returns:
            The same quotation after both dimensional labels agree.
        """
        expected = (
            "quote_per_spv_token"
            if self.quantity_convention == "spv_token"
            else "quote_per_underlying_equity"
        )
        if self.price_convention != expected:
            raise ValueError("xStocks price and quantity conventions do not agree")
        return self


def _require(condition: bool, reason: str) -> None:
    """Refuse a proposed interpretation before returning any converted value."""
    if not condition:
        raise XStockRebaseError(reason)


def exact_xstock_decimal(value: Fraction) -> Decimal:
    """Represent a finite rational exactly without using ambient Decimal precision.

    Args:
        value: Exact internal result, constructed from bounded decimal operands.

    Returns:
        A finite Decimal with no quantization or context rounding.
    """
    denominator = value.denominator
    twos = 0
    while denominator % 2 == 0:
        denominator //= 2
        twos += 1
    fives = 0
    while denominator % 5 == 0:
        denominator //= 5
        fives += 1
    _require(denominator == 1, "xstock_conversion_requires_nonterminating_decimal")
    scale = max(twos, fives)
    numerator = value.numerator * 2 ** (scale - twos) * 5 ** (scale - fives)
    digits = [int(digit) for digit in str(abs(numerator))]
    exponent = -scale
    while len(digits) > 1 and digits[-1] == 0:
        digits.pop()
        exponent += 1
    return Decimal((int(numerator < 0), tuple(digits), exponent))


def require_xstock_multiplier(
    witness: XStockMultiplierWitness, scope: XStockUnitScope, clocks: XStockInterpretation
) -> XStockMultiplierWitness:
    """Validate scope, economic effectivity and available knowledge independently.

    Args:
        witness: The exact supplied provider observation to use.
        scope: The actual instrument/asset/quote scope being interpreted.
        clocks: Economic observation and interpretation clocks.

    Returns:
        Revalidated evidence without guessing missing historical multipliers.
    """
    evidence = XStockMultiplierWitness.model_validate(witness)
    actual_scope = XStockUnitScope.model_validate(scope)
    context = XStockInterpretation.model_validate(clocks)
    _require(evidence.scope == actual_scope, "xstock_multiplier_scope_mismatch")
    _require(evidence.known_at <= context.interpreted_at, "xstock_multiplier_not_yet_known")
    _require(
        evidence.effective_from <= context.economic_at < evidence.effective_until,
        "xstock_multiplier_not_effective",
    )
    return evidence


def convert_xstock_units(
    value: XStockUnitValue,
    witness: XStockMultiplierWitness,
    clocks: XStockInterpretation,
    target: XStockQuantityConvention,
) -> XStockUnitValue:
    """Convert corresponding quantity and price while preserving exact quote notional.

    Args:
        value: An explicitly unit-tagged quotation from a proven endpoint profile.
        witness: Provider multiplier evidence for this scope and economic time.
        clocks: Economic and interpretation clocks, independent of fetch defaults.
        target: The requested SPV-token or underlying-equity representation.

    Returns:
        Exact converted operands; an inexact inverse requires new raw evidence.
    """
    reported = XStockUnitValue.model_validate(value)
    evidence = require_xstock_multiplier(witness, reported.scope, clocks)
    _require(target in {"spv_token", "underlying_equity"}, "xstock_target_unit_unsupported")
    if reported.quantity_convention == target:
        return reported
    multiplier = Fraction(evidence.multiplier)
    ratio = multiplier if target == "underlying_equity" else 1 / multiplier
    price_convention: XStockPriceConvention = (
        "quote_per_spv_token" if target == "spv_token" else "quote_per_underlying_equity"
    )
    return XStockUnitValue(
        scope=reported.scope,
        quantity_convention=target,
        price_convention=price_convention,
        quantity=exact_xstock_decimal(Fraction(reported.quantity) * ratio),
        price=exact_xstock_decimal(Fraction(reported.price) / ratio),
    )


def xstock_quote_notional(value: XStockUnitValue) -> Decimal:
    """Compute mathematical quote value without interpreting it as account cash.

    Args:
        value: Exact corresponding quantity and quote-price operands.

    Returns:
        Their exact product, independent of the caller's Decimal context.
    """
    evidence = XStockUnitValue.model_validate(value)
    return exact_xstock_decimal(Fraction(evidence.quantity) * Fraction(evidence.price))


def xstock_multiplier_digest(witness: XStockMultiplierWitness) -> str:
    """Identify the entire immutable multiplier witness, including its source clocks.

    Args:
        witness: The exact source observation and its explicit provenance.

    Returns:
        Versioned content digest, separate from provider and insertion identities.
    """
    evidence = XStockMultiplierWitness.model_validate(witness)
    material = json.dumps(evidence.model_dump(mode="json"), sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(XSTOCK_MULTIPLIER_HASH_DOMAIN + material.encode()).hexdigest()
