"""Original TradeVolume source interpretation preserves units, ambiguity and privacy boundaries."""

import json
from decimal import Decimal

import pytest

from snapper.application.trade.kraken_trade_fees import KrakenFeeDecimal
from snapper.application.trade.kraken_trade_fees import KrakenFeeOptional
from snapper.application.trade.kraken_trade_fees import KrakenTradeFeeObservation
from snapper.application.trade.kraken_trade_fees import parse_kraken_trade_fees


def _current() -> dict[str, object]:
    """Supply actual documented required fee-map fields with literal percentage strings."""
    return {"fee": "0.0500", "minfee": "0.0100", "maxfee": "0.0500", "tiervolume": "0"}


def _tier() -> dict[str, object]:
    """Supply actual documented required tier fields without inventing an activity flag."""
    return {"maker_fee": "-0.0020", "taker_fee": "0.0500"}


def _schedule() -> dict[str, object]:
    """Retain an actual class-qualified Futures symbol without claiming authenticated ownership."""
    return {"pair": "PF_XBTUSD", "class": "derivatives", "tiers": [_tier()]}


def _result() -> dict[str, object]:
    """Build a complete synthetic original source with separate current maps and full schedules."""
    return {
        "currency": "ZUSD",
        "volume": "200.0000",
        "inputs": {
            "domain_spot_volume_30d": "200.0000",
            "domain_futures_volume_30d": "0.0000",
            "domain_assets_on_platform": "2.5000",
        },
        "fees": {"PF_XBTUSD": _current()},
        "fees_maker": {"PF_XBTUSD": _current() | {"fee": "-0.0020"}},
        "schedules": [_schedule()],
    }


def _wire(result: dict[str, object]) -> bytes:
    """Encode source bytes before the actual decoder without constructing parser output models."""
    return json.dumps({"error": [], "result": result}, separators=(",", ":")).encode()


def test_original_fee_units_strings_and_schedule_order_survive_without_inferred_class() -> None:
    """Preserve current percentages and all tiers without selecting a preferred fee source.

    Given: Complete original maps and two schedule tiers include a signed maker rebate.
    When: The complete original TradeVolume entity is interpreted.
    Then: Decimal precision, source labels and order survive without account, clock or funds fields.
    """
    result = _result()
    result["schedules"] = [
        _schedule() | {"tiers": [_tier(), _tier() | {"taker_fee": "0.0200", "active": True}]}
    ]
    original = _wire(result)
    parsed = parse_kraken_trade_fees(original)
    assert parsed.taker_fees.value is not None and parsed.maker_fees.value is not None
    current = parsed.taker_fees.value[0]
    assert current.pair == "PF_XBTUSD"
    assert current.fee_percent.original_text == "0.0500"
    assert current.fee_percent.value == Decimal("0.0500")
    assert current.fee_percent.value.as_tuple().exponent == -4
    assert "asset_class" not in current.model_dump()
    assert parsed.maker_fees.value[0].fee_percent.value == Decimal("-0.0020")
    assert parsed.schedules.value is not None
    schedule = parsed.schedules.value[0]
    assert (schedule.pair, schedule.asset_class) == ("PF_XBTUSD", "derivatives")
    assert [tier.taker_fee_percent.value for tier in schedule.tiers] == [
        Decimal("0.0500"),
        Decimal("0.0200"),
    ]
    assert not schedule.tiers[0].active.present
    assert schedule.tiers[1].active.value is True
    assert "server_time" not in parsed.model_dump()
    assert "account_uid" not in parsed.model_dump()
    assert "available_margin" not in parsed.model_dump()
    assert original == _wire(result)


@pytest.mark.parametrize(
    ("present", "value"), [(False, None), (True, None), (True, False), (True, True)]
)
def test_provider_active_flag_keeps_all_four_original_states(
    present: bool, value: bool | None
) -> None:
    """Treat provider activity as an optional observation without defaulting absence to false.

    Given: A genuine schema-shaped tier has an absent, null, false or true activity member.
    When: The original entity passes through the fee source parser.
    Then: The original presence and Boolean value remain separate and no active tier is selected.
    """
    tier = _tier()
    if present:
        tier["active"] = value
    result = _result() | {"schedules": [_schedule() | {"tiers": [tier]}]}
    parsed = parse_kraken_trade_fees(_wire(result))
    assert parsed.schedules.value is not None
    active = parsed.schedules.value[0].tiers[0].active
    assert (active.present, active.value) == (present, value)


@pytest.mark.parametrize("field", ["fees", "fees_maker", "schedules", "asset_class"])
@pytest.mark.parametrize("state", ["absent", "null", "value"])
def test_optional_sections_preserve_absence_null_and_empty_source_values(
    field: str, state: str
) -> None:
    """Keep unavailable fee sections distinct from a provider's explicit empty observation.

    Given: One optional response section is absent, null or present with its supported empty value.
    When: The otherwise complete original response is parsed.
    Then: No fee, class or schedule default is manufactured for that section.
    """
    result = _result()
    result.pop(field, None)
    if state == "null":
        result[field] = None
    elif state == "value":
        result[field] = (
            "derivatives" if field == "asset_class" else [] if field == "schedules" else {}
        )
    parsed = parse_kraken_trade_fees(_wire(result))
    name = {
        "fees": "taker_fees",
        "fees_maker": "maker_fees",
        "asset_class": "volume_asset_class",
    }.get(field, field)
    observed: KrakenFeeOptional[object] = getattr(parsed, name)
    assert observed.present == (state != "absent")
    expected: object = None if state != "value" else "derivatives" if field == "asset_class" else ()
    assert observed.value == expected


@pytest.mark.parametrize(
    "field", ["nextfee", "tierfuturesvolume", "nextvolume", "nextfuturesvolume", "volumeoffset"]
)
@pytest.mark.parametrize("state", ["absent", "null", "value"])
def test_optional_current_fee_numbers_preserve_original_source_state(
    field: str, state: str
) -> None:
    """Keep nullable fee and volume boundaries separate from zero or a chosen next tier.

    Given: A documented optional current-fee member is absent, null or a decimal string.
    When: Its complete original pair-map entry is parsed.
    Then: The corresponding typed observation retains the original presence and exact value.
    """
    current = _current()
    if state != "absent":
        current[field] = None if state == "null" else "12.0300"
    parsed = parse_kraken_trade_fees(_wire(_result() | {"fees": {"PF_XBTUSD": current}}))
    assert parsed.taker_fees.value is not None
    name = {
        "nextfee": "next_fee_percent",
        "tierfuturesvolume": "futures_volume_tier",
        "nextvolume": "next_spot_volume_tier",
        "nextfuturesvolume": "next_futures_volume_tier",
        "volumeoffset": "volume_offset",
    }[field]
    observed: KrakenFeeOptional[KrakenFeeDecimal] = getattr(parsed.taker_fees.value[0], name)
    assert observed.present == (state != "absent")
    if state == "value":
        assert observed.value is not None
        assert observed.value.original_text == "12.0300"
        assert observed.value.value == Decimal("12.0300")
    else:
        assert observed.value is None


@pytest.mark.parametrize(
    "value",
    [
        None,
        True,
        1,
        0.05,
        "NaN",
        "Infinity",
        "1_2",
        " 1",
        "1 ",
        "01",
        ".5",
        "1.",
        "1e",
        "1e999999999999999999999999",
    ],
)
def test_non_decimal_or_unsupported_original_fee_text_is_refused(value: object) -> None:
    """Reject coercion and unsupported decimal syntax before a fee observation can be retained.

    Given: An otherwise complete original response has a malformed or non-string fee value.
    When: The actual original entity is interpreted.
    Then: Parsing refuses without converting a Boolean, float, nonfinite value or malformed text.
    """
    result = _result() | {"fees": {"PF_XBTUSD": _current() | {"fee": value}}}
    with pytest.raises(ValueError):
        parse_kraken_trade_fees(_wire(result))


@pytest.mark.parametrize("field", ["fee", "minfee", "maxfee", "tiervolume"])
def test_required_current_fee_field_is_never_silently_defaulted(field: str) -> None:
    """Require every documented current-fee operand before accepting a complete pair map.

    Given: An original current-fee row omits one required schema member.
    When: The complete response is parsed.
    Then: The entire interpretation refuses instead of keeping a partial successful fee source.
    """
    current = _current()
    del current[field]
    with pytest.raises(ValueError):
        parse_kraken_trade_fees(_wire(_result() | {"fees": {"PF_XBTUSD": current}}))


@pytest.mark.parametrize(
    "field",
    [
        "Authent",
        " nonce ",
        "PRIVATEKEY",
        "ApiSecret",
        "api_key",
        "API-Key",
        "password",
        "python-kraken-sdk",
    ],
)
@pytest.mark.parametrize("nested", [False, True])
def test_otherwise_valid_fees_refuse_private_fields_at_any_depth(field: str, nested: bool) -> None:
    """Inspect unknown source members before original fee bytes could become durable evidence.

    Given: An otherwise valid source contains a synthetic authentication value at the root or nested.
    When: The complete original fee entity is interpreted.
    Then: Parsing refuses with a fixed category that never includes the private value.
    """
    document: dict[str, object] = {"error": [], "result": _result()}
    if nested:
        document["unknown"] = [{"safe": {field: "synthetic-private-fee-marker"}}]
    else:
        document[field] = "synthetic-private-fee-marker"
    with pytest.raises(ValueError, match="private_fields_refused") as error:
        parse_kraken_trade_fees(json.dumps(document).encode())
    assert "synthetic-private-fee-marker" not in str(error.value)


@pytest.mark.parametrize("member", [{"event": "challenge"}, {"method": "subscribe"}])
def test_control_envelopes_cannot_be_relabelled_as_successful_fee_sources(
    member: dict[str, str],
) -> None:
    """Keep authentication control data out of otherwise valid financial response shapes.

    Given: A successful-looking fee response contains a recognized control member.
    When: The original source is inspected before projection.
    Then: The parser refuses without accepting its otherwise complete fee fields.
    """
    document = {"error": [], "result": _result(), **member}
    with pytest.raises(ValueError, match="control_source_refused"):
        parse_kraken_trade_fees(json.dumps(document).encode())


@pytest.mark.parametrize(
    "wire",
    [
        b"[]",
        b"{}",
        b'{"error":[],"error":[]}',
        b'{"error":[],"result":NaN}',
        b'{"error":["EGeneral:synthetic"],"result":{}}',
        b'{"error":null,"result":{}}',
        b"\xff",
        b'{"error":[]',
    ],
)
def test_invalid_or_unsuccessful_original_envelope_cannot_supply_fee_evidence(wire: bytes) -> None:
    """Require complete unique-key JSON and explicit successful Spot response status.

    Given: Original bytes are malformed, ambiguous, nonfinite, non-object or unsuccessful.
    When: The actual original-byte parser reads them.
    Then: No partial fee observation or empty successful substitute is returned.
    """
    with pytest.raises(ValueError):
        parse_kraken_trade_fees(wire)


@pytest.mark.parametrize("encoding", ["utf-16-le", "utf-16-be", "utf-32-le", "utf-32-be"])
def test_json_encoding_autodetection_cannot_accept_non_utf8_fee_sources(encoding: str) -> None:
    """Require actual UTF-8 bytes even when a JSON decoder supports other encodings.

    Given: A complete valid fee document is encoded as BOMless UTF-16 or UTF-32.
    When: Those original bytes reach the public parser.
    Then: The source refuses instead of accepting an ASCII-NUL encoding through autodetection.
    """
    wire = _wire(_result()).decode().encode(encoding)
    with pytest.raises(ValueError, match="original_entity_required"):
        parse_kraken_trade_fees(wire)


def test_duplicate_schedule_observations_are_retained_without_local_tier_resolution() -> None:
    """Preserve source ambiguity instead of picking or merging duplicate schedule entries.

    Given: The original response includes two class-qualified schedules for the same pair.
    When: Observation-only parsing reads their different original fee values.
    Then: Both remain in original order and no active schedule or current fee is inferred.
    """
    result = _result() | {
        "schedules": [_schedule(), _schedule() | {"tiers": [_tier() | {"taker_fee": "0.0400"}]}]
    }
    parsed = parse_kraken_trade_fees(_wire(result))
    assert parsed.schedules.value is not None and len(parsed.schedules.value) == 2
    assert [item.tiers[0].taker_fee_percent.value for item in parsed.schedules.value] == [
        Decimal("0.0500"),
        Decimal("0.0400"),
    ]


@pytest.mark.parametrize("kind", ["bytes", "nodes", "depth", "map", "tiers", "schedules"])
def test_explicit_local_resource_bounds_refuse_oversized_source_interpretations(kind: str) -> None:
    """Bound source interpretation without pretending local limits are exchange guarantees.

    Given: One original entity exceeds the supported byte, shape, entry or nesting bound.
    When: The actual complete-source parser inspects it.
    Then: It refuses the unsupported source without truncating an array into apparent completeness.
    """
    result = _result()
    if kind == "nodes":
        result["unknown"] = [0] * 65536
    elif kind == "depth":
        nested: object = 0
        for _ in range(65):
            nested = [nested]
        result["unknown"] = nested
    elif kind == "map":
        result["fees"] = {f"PAIR{index}": _current() for index in range(4097)}
    elif kind == "tiers":
        result["schedules"] = [_schedule() | {"tiers": [_tier()] * 4097}]
    elif kind == "schedules":
        result["schedules"] = [_schedule()] * 4097
    wire = b" " * (8 * 1024 * 1024 + 1) if kind == "bytes" else _wire(result)
    with pytest.raises(ValueError):
        parse_kraken_trade_fees(wire)


def test_model_revalidation_rejects_changed_numeric_copy_and_absent_value_claim() -> None:
    """Keep model copies consistent without treating constructed models as actual source issuance.

    Given: A genuine parser result and separately constructed contradictory presence/numeric copies.
    When: Strict models revalidate those public observations.
    Then: They reject the contradictions without granting or fabricating any transport proof.
    """
    parsed = parse_kraken_trade_fees(_wire(_result()))
    changed = parsed.volume.model_copy(update={"value": Decimal("999")})
    with pytest.raises(ValueError, match="decimal_copy_mismatch"):
        KrakenTradeFeeObservation.model_validate(parsed.model_copy(update={"volume": changed}))
    with pytest.raises(ValueError, match="absent_value_supplied"):
        KrakenFeeOptional[bool](present=False, value=True)


@pytest.mark.parametrize(
    ("field", "value", "reason"),
    [
        ("result", [], "object_required"),
        ("fees", [], "object_required"),
        ("schedules", {}, "array_required"),
        ("active", 1, "boolean_required"),
        ("active", "false", "boolean_required"),
        ("currency", "\ud800", "text_not_unicode"),
    ],
)
def test_original_response_shapes_flags_and_unicode_cannot_be_coerced_into_fees(
    field: str, value: object, reason: str
) -> None:
    """Refuse malformed original structures without replacing them with successful defaults.

    Given: An otherwise complete original response has one invalid section, activity flag or scalar.
    When: The public parser reads the actual encoded JSON bytes, including an escaped lone surrogate.
    Then: It rejects the source with the precise fixed category instead of coercing a financial value.
    """
    result = _result()
    document: dict[str, object] = {"error": [], "result": result}
    if field == "result":
        document["result"] = value
    elif field == "active":
        result["schedules"] = [_schedule() | {"tiers": [_tier() | {"active": value}]}]
    else:
        result[field] = value
    original = json.dumps(document, ensure_ascii=True, separators=(",", ":")).encode()
    with pytest.raises(ValueError, match=f"kraken_fee_{reason}"):
        parse_kraken_trade_fees(original)
