"""Literal account-resolved margin schema interpretation without account or funds fabrication."""

import json
from decimal import Decimal

import pytest

from snapper.application.trade.futures_margin_schedule import FuturesMarginSchedule
from snapper.application.trade.futures_margin_schedule import parse_futures_margin_schedule


def _tier() -> dict[str, object]:
    """Supply original multi-collateral threshold and numeric rate fields only."""
    return {"numNonContractUnits": 0, "initialMargin": 0.02, "maintenanceMargin": 0.01}


def _row() -> dict[str, object]:
    """Provide one literal authenticated-schema row without claiming actual source issuance."""
    return {
        "symbol": "PF_XBTUSD",
        "type": "flexible_futures",
        "isExpired": False,
        "suspended": False,
        "postOnly": False,
        "restricted": False,
        "marginLevels": [_tier()],
    }


def _wire(rows: list[dict[str, object]]) -> bytes:
    """Encode complete synthetic source bytes before the actual exact-decimal parser boundary."""
    return json.dumps(
        {
            "result": "success",
            "serverTime": "2026-10-24T00:00:00Z",
            "instruments": rows,
        },
        separators=(",", ":"),
    ).encode()


@pytest.mark.parametrize(
    ("kind", "threshold_field"),
    [
        ("flexible_futures", "numNonContractUnits"),
        ("futures_inverse", "contracts"),
        ("futures_vanilla", "contracts"),
    ],
)
def test_literal_margin_schedule_preserves_tier_order_and_explicit_threshold_units(
    kind: str, threshold_field: str
) -> None:
    """Keep original per-product units and tier observations without computing required margin.

    Given: An exact selected future row contains two complete ascending margin levels.
    When: The original entity is interpreted with the authenticated schedule schema.
    Then: Tier order, exact rates and threshold units remain intact without an available-funds field.
    """
    row = _row()
    row["type"] = kind
    row["marginLevels"] = [
        {threshold_field: 0, "initialMargin": 0.02, "maintenanceMargin": 0.01},
        {threshold_field: 100, "initialMargin": 0.04, "maintenanceMargin": 0.02},
    ]
    schedule = parse_futures_margin_schedule(_wire([row]), "PF_XBTUSD")
    assert schedule.threshold_field == threshold_field
    assert schedule.instrument_type == kind
    assert [tier.threshold for tier in schedule.tiers] == [Decimal(0), Decimal(100)]
    assert [tier.initial_margin_rate for tier in schedule.tiers] == [
        Decimal("0.02"),
        Decimal("0.04"),
    ]
    assert "available_margin" not in schedule.model_dump()


def test_original_decimal_value_and_exponent_survive_without_a_binary_float_roundtrip() -> None:
    """Read exact original numeric tokens without passing through a JSON floating-point value.

    Given: Original response bytes contain a trailing-zero rate and a high-precision decimal threshold.
    When: The actual complete-source and schedule parsers decode those unchanged bytes.
    Then: Exact Decimal value and exponent survive, and the input entity remains unchanged.
    """
    row = _row()
    row["marginLevels"] = [
        {
            "numNonContractUnits": "precise-threshold",
            "initialMargin": "precise-rate",
            "maintenanceMargin": 0,
        }
    ]
    wire = _wire([row]).replace(b'"precise-threshold"', b"0.123456789012345678901234567890")
    wire = wire.replace(b'"precise-rate"', b"0.10")
    original = bytes(wire)
    schedule = parse_futures_margin_schedule(wire, "PF_XBTUSD")
    assert str(schedule.tiers[0].threshold) == "0.123456789012345678901234567890"
    assert str(schedule.tiers[0].initial_margin_rate) == "0.10"
    assert wire == original


@pytest.mark.parametrize(
    ("state", "expected"),
    [
        ("absent", "schedule_absent"),
        ("null", "schedule_array_required"),
        ("object", "schedule_array_required"),
        ("string", "schedule_array_required"),
        ("empty", "schedule_empty"),
    ],
)
def test_missing_or_unusable_authenticated_schedule_never_falls_back_to_public_tiers(
    state: str, expected: str
) -> None:
    """Refuse unavailable account-resolved tiers even when public retail and platform tiers exist.

    Given: The selected row has public alternative fields but no usable authenticated marginLevels.
    When: The original response is interpreted for the authenticated account schedule.
    Then: The exact absent, empty or malformed state refuses without consulting public alternatives.
    """
    row = _row()
    row["retailMarginLevels"] = [_tier()]
    row["marginSchedules"] = {"mtf": {"retail": [_tier()]}}
    alternatives: dict[str, object] = {"null": None, "object": {}, "string": "missing", "empty": []}
    if state == "absent":
        row.pop("marginLevels")
    else:
        row["marginLevels"] = alternatives[state]
    with pytest.raises(ValueError, match=expected):
        parse_futures_margin_schedule(_wire([row]), "PF_XBTUSD")


@pytest.mark.parametrize("layout", ["both", "neither", "mixed", "non_object"])
def test_ambiguous_or_incompatible_threshold_units_refuse_without_conversion(layout: str) -> None:
    """Keep per-tier threshold dimensions explicit rather than treating one numeric field as another.

    Given: A selected multi-collateral schedule has ambiguous, mixed or incompatible original units.
    When: Its actual original margin array is interpreted.
    Then: The schedule refuses instead of dropping a field, converting a unit or accepting a partial list.
    """
    first = _tier()
    levels: list[object] = [first]
    if layout == "both":
        first["contracts"] = 0
    elif layout == "neither":
        first.pop("numNonContractUnits")
    elif layout == "mixed":
        levels.append({"contracts": 10, "initialMargin": 0.04, "maintenanceMargin": 0.02})
    else:
        levels.append("not-an-original-tier-object")
    row = _row()
    row["marginLevels"] = levels
    with pytest.raises(ValueError, match="futures_margin_"):
        parse_futures_margin_schedule(_wire([row]), "PF_XBTUSD")


@pytest.mark.parametrize("kind", ["futures_inverse", "futures_vanilla", "flexible_futures"])
def test_observed_threshold_field_is_not_inferred_from_the_instrument_type(kind: str) -> None:
    """Preserve the original threshold dimension pending independent real account/product binding.

    Given: A supported instrument type carries an explicit non-contract threshold field.
    When: The pure evidence projection interprets that original row.
    Then: It preserves the field verbatim without inventing account mode or native order-quantity units.
    """
    row = _row()
    row["type"] = kind
    observed = parse_futures_margin_schedule(_wire([row]), "PF_XBTUSD")
    assert observed.threshold_field == "numNonContractUnits"
    assert observed.instrument_type == kind


@pytest.mark.parametrize("field", ["numNonContractUnits", "initialMargin", "maintenanceMargin"])
@pytest.mark.parametrize("value", [-1, True, "0.1", None, []])
def test_numeric_tier_operands_require_original_nonnegative_json_numbers(
    field: str, value: object
) -> None:
    """Refuse malformed numeric operands without converting textual, Boolean or absent values.

    Given: One otherwise complete tier contains a negative or nonnumeric threshold or rate.
    When: The actual source parser reaches the selected margin projection.
    Then: Interpretation refuses without a zero default or string-to-number conversion.
    """
    tier = _tier()
    tier[field] = value
    row = _row()
    row["marginLevels"] = [tier]
    with pytest.raises(ValueError, match="futures_margin_"):
        parse_futures_margin_schedule(_wire([row]), "PF_XBTUSD")


@pytest.mark.parametrize("thresholds", [[0, 0], [10, 0]])
def test_duplicate_or_descending_thresholds_refuse_under_explicit_supported_layout_policy(
    thresholds: list[int],
) -> None:
    """Refuse ambiguous local interpretation without sorting or merging the provider's original tiers.

    Given: A complete original schedule contains equal or descending threshold observations.
    When: The parser applies its explicitly supported ascending-layout policy.
    Then: It refuses while leaving the original source order untouched.
    """
    row = _row()
    row["marginLevels"] = [dict(_tier(), numNonContractUnits=value) for value in thresholds]
    wire = _wire([row])
    with pytest.raises(ValueError, match="threshold_order_unsupported"):
        parse_futures_margin_schedule(wire, "PF_XBTUSD")


@pytest.mark.parametrize("threshold", [0.5, 2**64])
def test_contract_threshold_obeys_original_uint64_domain(threshold: int | float) -> None:
    """Keep contract counts integral and within the provider schema's actual unsigned domain.

    Given: A single-collateral original tier has a fractional or out-of-range contract threshold.
    When: The selected complete schedule is parsed using its contract unit.
    Then: It refuses rather than rounding or accepting an unsupported count.
    """
    row = _row()
    row["type"] = "futures_inverse"
    row["marginLevels"] = [
        {"contracts": threshold, "initialMargin": 0.02, "maintenanceMargin": 0.01}
    ]
    with pytest.raises(ValueError, match="contract_threshold_invalid"):
        parse_futures_margin_schedule(_wire([row]), "PF_XBTUSD")


@pytest.mark.parametrize("symbol", ["pf_xbtusd", " PF_XBTUSD", "UNKNOWN"])
def test_exact_symbol_selection_cannot_normalize_to_another_schedule(symbol: str) -> None:
    """Keep original product identity distinct from a convenient normalized or substituted symbol.

    Given: The original complete list contains only one exact exchange symbol.
    When: A different spelling or absent symbol is selected.
    Then: The parser refuses without selecting the available row.
    """
    with pytest.raises(ValueError, match="exact_symbol_missing"):
        parse_futures_margin_schedule(_wire([_row()]), symbol)


def test_selected_nonfirst_symbol_retains_its_own_original_rates() -> None:
    """Select the exact original row without accidentally retaining the first schedule in the list.

    Given: Two distinct valid product rows have different original margin rates.
    When: The second exact symbol is selected.
    Then: Only that row's literal rate is returned and the other schedule cannot substitute for it.
    """
    other = _row()
    other["symbol"] = "PF_ETHUSD"
    selected = _row()
    selected["marginLevels"] = [dict(_tier(), initialMargin=0.3)]
    schedule = parse_futures_margin_schedule(_wire([other, selected]), "PF_XBTUSD")
    assert schedule.tiers[0].initial_margin_rate == Decimal("0.3")


@pytest.mark.parametrize("kind", ["options", "future_unknown"])
def test_nonfuture_or_unknown_product_types_cannot_supply_the_supported_schedule(kind: str) -> None:
    """Keep an identically shaped array from proving unsupported contract-family semantics.

    Given: The selected original row has an option or unrecognized instrument type.
    When: Its otherwise well-formed margin array is parsed by the futures schedule reader.
    Then: Interpretation refuses without inferring a futures family from the symbol or field names.
    """
    row = _row()
    row["type"] = kind
    with pytest.raises(ValueError, match="future_type_unsupported"):
        parse_futures_margin_schedule(_wire([row]), "PF_XBTUSD")


def test_oversized_schedule_refuses_under_explicit_local_resource_policy() -> None:
    """Apply the local complete-schedule bound without retaining an accepted partial prefix.

    Given: A complete otherwise valid original row contains more tiers than the supported local limit.
    When: The actual parser processes that source.
    Then: It refuses the whole schedule rather than truncating the original array.
    """
    row = _row()
    row["marginLevels"] = [dict(_tier(), numNonContractUnits=value) for value in range(4097)]
    with pytest.raises(ValueError, match="schedule_size_unsupported"):
        parse_futures_margin_schedule(_wire([row]), "PF_XBTUSD")


def test_public_unchecked_nested_mutation_is_revalidated_before_schedule_reuse() -> None:
    """Keep the strict observation boundary effective after low-level mutation of a frozen child.

    Given: A genuine parsed schedule has its public nested threshold altered through object.__setattr__.
    When: The public model is revalidated for reuse.
    Then: The invalid child cannot survive the immutable model boundary.
    """
    schedule = parse_futures_margin_schedule(_wire([_row()]), "PF_XBTUSD")
    object.__setattr__(schedule.tiers[0], "threshold", Decimal(-1))
    with pytest.raises(ValueError):
        FuturesMarginSchedule.model_validate(schedule)
