"""Exact endpoint-specific flex risk interpretation preserves amounts without inventing availability."""

import base64
import json
from datetime import UTC
from datetime import datetime
from decimal import Decimal

import pytest

from snapper.application.trade.futures_flex_risk import FuturesFlexRiskSourceError
from snapper.application.trade.futures_flex_risk import parse_futures_flex_risk
from snapper.application.trade.futures_native_accounts import FUTURES_NATIVE_ACCOUNTS_CHANNEL
from snapper.application.trade.futures_native_accounts import FUTURES_PREPARED_ACCOUNTS_CHANNEL
from snapper.core.contract_accounting_observation import AccountingObservationScope
from snapper.core.contract_accounting_observation import RawAccountingObservation
from snapper.core.contract_accounting_observation import canonical_accounting_observation

_WIRE = (
    b'{"result":"success","serverTime":"2026-10-23T21:00:00.125Z","accounts":{'
    b'"cash":{"type":"cashAccount","balances":{}},'
    b'"flex":{"type":"multiCollateralMarginAccount","currencies":{'
    b'"XBT":{"quantity":0.003000000000000000001,"value":777,"collateral":600,"available":-0.001},'
    b'"USD":{"quantity":-1.25,"value":-1.25,"collateral":-1,"available":-2.5}},'
    b'"initialMargin":100,"initialMarginWithOrders":125,"maintenanceMargin":50,'
    b'"balanceValue":400,"portfolioValue":385,"collateralValue":350,"pnl":-15,'
    b'"unrealizedFunding":-2,"totalUnrealized":-17,"totalUnrealizedAsMargin":-18,'
    b'"availableMargin":-1,"marginEquity":332,"futureMetadata":{"keep":"original-risk-marker"}}}}'
)


def _raw(wire: bytes = _WIRE, **updates: object) -> RawAccountingObservation:
    """Supply synthetic original source bytes without claiming transport issuance or account authority."""
    observation = RawAccountingObservation(
        scope=AccountingObservationScope(
            wallet_public_id="00000000-0000-7000-8000-000000001001",
            exchange="kraken_futures",
            mode="live",
            environment="demo",
        ),
        route_revision="synthetic-risk-fixture",
        kind="account_cash",
        channel=FUTURES_PREPARED_ACCOUNTS_CHANNEL,
        representation="wire_bytes_base64",
        material=base64.b64encode(wire).decode("ascii"),
        observed_at=datetime(2026, 10, 23, 21, 0, 1, tzinfo=UTC),
    )
    return RawAccountingObservation.model_validate({**observation.model_dump(), **updates})


def test_exact_flex_fields_preserve_currency_units_and_independent_signed_usd_values() -> None:
    """Keep provider risk observations distinct from native cash and calculated reservation capacity.

    Given: Original flex source with exact fractional units, signed values and unequal margin fields.
    When: The additive endpoint-specific risk projection reads the complete source.
    Then: Every amount retains its own unit and value without equations, clamping or source rewriting.
    """
    original = _raw()
    before = canonical_accounting_observation(original)
    result = parse_futures_flex_risk(original)
    assert result.source_policy == "kraken_futures_flex_risk_v1"
    assert result.native_account_key == "flex"
    assert result.server_time_text == "2026-10-23T21:00:00.125Z"
    assert result.server_time == datetime(2026, 10, 23, 21, 0, 0, 125000, tzinfo=UTC)
    assert result.margin_usd.model_dump() == {
        "initial_margin_usd": Decimal(100),
        "initial_margin_with_orders_usd": Decimal(125),
        "maintenance_margin_usd": Decimal(50),
        "balance_value_usd": Decimal(400),
        "portfolio_value_usd": Decimal(385),
        "collateral_value_usd": Decimal(350),
        "unrealized_pnl_usd": Decimal(-15),
        "unrealized_funding_usd": Decimal(-2),
        "total_unrealized_usd": Decimal(-17),
        "total_unrealized_as_margin_usd": Decimal(-18),
        "available_margin_usd": Decimal(-1),
        "margin_equity_usd": Decimal(332),
    }
    assert result.currencies[0].model_dump() == {
        "currency": "XBT",
        "quantity_native": Decimal("0.003000000000000000001"),
        "value_usd": Decimal(777),
        "collateral_usd": Decimal(600),
        "available_native": Decimal("-0.001"),
    }
    assert result.currencies[1].currency == "USD"
    assert result.currencies[1].quantity_native == Decimal("-1.25")
    assert result.currencies[1].available_native == Decimal("-2.5")
    assert canonical_accounting_observation(original) == before
    assert b"original-risk-marker" in base64.b64decode(original.material)


@pytest.mark.parametrize(
    "field",
    [
        "initialMargin",
        "initialMarginWithOrders",
        "maintenanceMargin",
        "balanceValue",
        "portfolioValue",
        "collateralValue",
        "pnl",
        "unrealizedFunding",
        "totalUnrealized",
        "totalUnrealizedAsMargin",
        "availableMargin",
        "marginEquity",
    ],
)
def test_missing_required_usd_field_never_becomes_zero_or_another_margin_operand(
    field: str,
) -> None:
    """Refuse incomplete endpoint risk data without filling a required amount from nearby values.

    Given: A successful source omits one required flex summary field while its other fields remain.
    When: The additive risk projection interprets that source.
    Then: The whole projection is refused instead of substituting zero or deriving an amount.
    """
    source = json.loads(_WIRE)
    del source["accounts"]["flex"][field]
    with pytest.raises(ValueError):
        parse_futures_flex_risk(_raw(json.dumps(source).encode()))


@pytest.mark.parametrize("field", ["quantity", "value", "collateral", "available"])
def test_missing_currency_operand_cannot_borrow_an_account_total(field: str) -> None:
    """Keep per-currency completeness independent of account-wide USD totals.

    Given: The selected currency is missing one documented operand but all flex totals are present.
    When: The original response is projected into risk observations.
    Then: No partial currency or borrowed account-total amount is returned.
    """
    source = json.loads(_WIRE)
    del source["accounts"]["flex"]["currencies"]["XBT"][field]
    with pytest.raises(ValueError):
        parse_futures_flex_risk(_raw(json.dumps(source).encode()))


@pytest.mark.parametrize("value", [b"true", b'"100"', b"null", b"NaN", b"Infinity"])
def test_risk_numbers_require_original_finite_json_numeric_operands(value: bytes) -> None:
    """Reject coercible and nonfinite risk operands before they enter the typed projection.

    Given: The original JSON supplies a boolean, string, null or nonfinite extension as margin.
    When: The complete source is interpreted.
    Then: The malformed amount is refused without permissive Decimal coercion.
    """
    wire = _WIRE.replace(b'"initialMargin":100', b'"initialMargin":' + value)
    with pytest.raises(ValueError):
        parse_futures_flex_risk(_raw(wire))


@pytest.mark.parametrize("replacement", [b'"FLEX"', b'"multi_collateral"'])
def test_alias_account_keys_cannot_supply_the_documented_flex_schema(replacement: bytes) -> None:
    """Require the endpoint's literal account identity instead of a familiar-looking alias.

    Given: A complete multi-collateral profile appears under an account key other than flex.
    When: The risk projection searches the original accounts inventory.
    Then: It refuses absence of the actual flex member and does not fall back to another profile.
    """
    with pytest.raises(ValueError):
        parse_futures_flex_risk(_raw(_WIRE.replace(b'"flex"', replacement)))


def test_flex_key_cannot_override_a_different_explicit_account_type() -> None:
    """Bind schema meaning to both literal account key and explicit account type.

    Given: A structurally valid cash account named flex contains no multi-collateral risk schema.
    When: The additive risk projection interprets the source.
    Then: The familiar key cannot promote the different account type into margin evidence.
    """
    source = json.loads(_WIRE)
    source["accounts"]["flex"] = {"type": "cashAccount", "balances": {}}
    with pytest.raises(FuturesFlexRiskSourceError, match="literal_account_type_required"):
        parse_futures_flex_risk(_raw(json.dumps(source).encode()))


def test_historical_unguarded_accounts_source_does_not_supply_new_risk_policy() -> None:
    """Keep historical source interpretation separate from the new guarded risk prerequisite.

    Given: Original valid account bytes carry the older unguarded REST channel.
    When: The new flex risk policy receives that historical observation.
    Then: It refuses the weaker source without rewriting or reclassifying historical evidence.
    """
    with pytest.raises(FuturesFlexRiskSourceError, match="guarded_accounts_source_required"):
        parse_futures_flex_risk(_raw(channel=FUTURES_NATIVE_ACCOUNTS_CHANNEL))


@pytest.mark.parametrize("error_field", ["error", "errors"])
def test_success_label_with_error_metadata_cannot_supply_risk_evidence(error_field: str) -> None:
    """Reject a contradictory envelope instead of trusting its successful result label alone.

    Given: A complete original account response also carries explicit error metadata.
    When: The risk projection examines its source envelope.
    Then: It refuses the contradiction before returning usable risk observations.
    """
    source = json.loads(_WIRE)
    source[error_field] = "synthetic-error-marker"
    with pytest.raises(FuturesFlexRiskSourceError, match="contradictory_success_envelope"):
        parse_futures_flex_risk(_raw(json.dumps(source).encode()))


@pytest.mark.parametrize("extra_bytes", [1, 4])
def test_original_entity_size_is_bounded_before_risk_decoding(extra_bytes: int) -> None:
    """Enforce the original-byte bound across both equal-length and larger base64 encodings.

    Given: Otherwise valid original JSON exceeds the eight MiB entity limit by one or four bytes.
    When: The risk projection receives its canonical base64 representation.
    Then: The source is refused at its actual encoded or decoded size boundary.
    """
    wire = _WIRE + b" " * (8 * 1024 * 1024 + extra_bytes - len(_WIRE))
    with pytest.raises(FuturesFlexRiskSourceError, match="original_source_too_large"):
        parse_futures_flex_risk(_raw(wire))


def test_duplicate_risk_fields_and_duplicate_currency_keys_are_refused() -> None:
    """Refuse ambiguous original JSON before a last-wins map can silently erase risk information.

    Given: Original source bytes repeat a margin field or an exact currency key.
    When: The strict accounts decoder supplies the risk projection.
    Then: Both ambiguous sources fail without selecting a more favorable value.
    """
    repeated_margin = _WIRE.replace(
        b'"initialMargin":100', b'"initialMargin":100,"initialMargin":0'
    )
    repeated_currency = _WIRE.replace(b'"USD":{"quantity":-1.25', b'"XBT":{"quantity":-1.25')
    for wire in (repeated_margin, repeated_currency):
        with pytest.raises(ValueError):
            parse_futures_flex_risk(_raw(wire))


def test_distinct_currency_spellings_and_signed_zero_remain_source_observations() -> None:
    """Preserve original currency identities and exact signed Decimal observations without aliases.

    Given: Original source contains distinct XBT and xbt currency keys and an explicit negative zero.
    When: The additive risk projection reads the original numeric tokens.
    Then: Both keys remain distinct and the parsed signed zero remains separate from unchanged raw bytes.
    """
    wire = _WIRE.replace(b'"USD":', b'"xbt":').replace(b'"quantity":-1.25', b'"quantity":-0.0')
    raw = _raw(wire)
    result = parse_futures_flex_risk(raw)
    assert tuple(currency.currency for currency in result.currencies) == ("XBT", "xbt")
    assert result.currencies[1].quantity_native.is_signed()
    assert result.currencies[1].quantity_native == Decimal(0)
    assert base64.b64decode(raw.material) == wire
