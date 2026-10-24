"""Strict snapshot evidence, exact canonical identities and receipt fidelity."""

import hashlib
import json
from collections.abc import Callable
from datetime import UTC
from datetime import datetime
from datetime import timedelta
from datetime import timezone
from decimal import Decimal
from decimal import Inexact
from decimal import Overflow
from decimal import Rounded
from decimal import Underflow
from decimal import localcontext
from typing import cast

import pytest
from pydantic import BaseModel
from pydantic import ValidationError
from pydantic_core import PydanticSerializationError

from snapper.core.execution_unit_snapshot import ExecutionUnitSnapshotPayload
from snapper.core.execution_unit_snapshot import FillSnapshotTarget
from snapper.core.execution_unit_snapshot import SnapshotCommandMaterial
from snapper.core.execution_unit_snapshot import SnapshotReceipt
from snapper.core.execution_unit_snapshot import SnapshotScope
from snapper.core.execution_unit_snapshot import SnapshotWriteProvenance
from snapper.core.execution_unit_snapshot import SubmissionSnapshotTarget
from snapper.core.execution_unit_snapshot import canonical_snapshot_payload
from snapper.core.execution_unit_snapshot import command_request_digest
from snapper.core.execution_unit_snapshot import receipt_content_digest
from snapper.core.execution_unit_snapshot import snapshot_content_digest

_WALLET_ID = "aabbccdd-eeff-4011-8233-445566778899"
_INSTRUMENT_ID = "11223344-5566-4788-99aa-bbccddeeff00"
_COMMAND_ID = "abcdefab-cdef-4123-8567-89abcdef0123"
_EXECUTION_ID = "01234567-89ab-4cde-8fab-0123456789ab"
_EVENT_ID = "fedcba98-7654-4321-8fed-cba987654321"
_OBSERVED = datetime(2026, 10, 22, 12, 30, 1, 123456, tzinfo=UTC)
_INTERPRETED = datetime(2026, 10, 22, 12, 30, 2, 123456, tzinfo=UTC)
_RECEIPT_TEXT = '{ "quantity": "2.5000", "price": 100.00 }'
_PAYOFF_FIELDS = (
    "payoff_model",
    "contract_multiplier",
    "contract_value_currency",
    "settlement_currency",
)
_INVALID_DECIMALS: tuple[object, ...] = (
    Decimal("NaN"),
    Decimal("sNaN"),
    Decimal("Infinity"),
    Decimal("-Infinity"),
    "NaN",
    "sNaN",
    "Infinity",
    "-Infinity",
    "not-a-decimal",
    True,
    False,
    0,
    1,
    1.25,
)


def _scope_data() -> dict[str, object]:
    """Provide one canonical account and instrument scope.

    Returns:
        Fresh strict-model input for the account and instrument scope.
    """
    return {
        "wallet_public_id": _WALLET_ID,
        "exchange": "kraken",
        "mode": "paper",
        "instrument_public_id": _INSTRUMENT_ID,
    }


def _dimensions_data() -> dict[str, object]:
    """Provide an exact linear-contract interpretation without any conversion.

    Returns:
        Fresh exact decimal operands and their explicit dimensions.
    """
    return {
        "base_currency": "BTC",
        "quote_currency": "USD",
        "native_order_quantity_unit": "contract_count",
        "internal_quantity_unit": "contract_count",
        "signed_quantity_unit": "contract_count",
        "side": "buy",
        "signed_quantity": "2.5000",
        "quantity_quantum": "0.00100",
        "price": "100.00",
        "stop_price": "-0.000",
        "price_quantum": "0.0100",
        "price_currency": "USD",
        "price_quantity_convention": "quote_per_base",
        "catalog_contract_multiplier": "0.1000",
        "fee": "0.0100",
        "fee_currency": "USD",
        "payoff_model": "linear",
        "contract_multiplier": "0.1000",
        "contract_value_currency": "BTC",
        "settlement_currency": "USD",
    }


def _receipt_data() -> dict[str, object]:
    """Preserve a decoded receipt's original spelling and observation provenance.

    Returns:
        Fresh tagged decoded receipt input with its exact material.
    """
    return {
        "representation": "decoded_json_text",
        "material": _RECEIPT_TEXT,
        "endpoint_or_channel": "private/order",
        "observed_at": _OBSERVED,
    }


def _payload_data() -> dict[str, object]:
    """Provide strict nested input containing no mutable model instances.

    Returns:
        Fresh complete nested submission-evidence input.
    """
    return {
        "schema_version": 1,
        "target": {
            "phase": "submission",
            "command_public_id": _COMMAND_ID,
            "client_order_id": "intent-17",
            "request_digest": "a" * 64,
        },
        "scope": _scope_data(),
        "catalog": {
            "spec_public_id": _INSTRUMENT_ID,
            "spec_timestamp": _OBSERVED,
            "source": "historical-specification",
            "version": "parser-v1",
            "observed_at": _OBSERVED,
        },
        "dimensions": _dimensions_data(),
        "venue_identity": {
            "native_symbol": "XBTUSD",
            "source": "instrument-parser",
            "source_version": "parser-v1",
            "observed_at": _OBSERVED,
            "raw_base_asset_id": "XXBT",
            "raw_quote_asset_id": "ZUSD",
            "display_base_asset": "XBT",
            "display_quote_asset": "USD",
            "altname": "XBTUSD",
            "wsname": "XBT/USD",
        },
        "rebase": {"quantity_convention": "not_applicable"},
        "receipt": _receipt_data(),
        "interpreted_at": _INTERPRETED,
    }


def _command_data() -> dict[str, object]:
    """Provide durable create-request material independent of command status.

    Returns:
        Fresh typed-model input containing all durable request operands.
    """
    return {
        "command_public_id": _COMMAND_ID,
        "wallet_public_id": _WALLET_ID,
        "exchange": "kraken",
        "mode": "paper",
        "native_symbol": "XBTUSD",
        "client_order_id": "intent-17",
        "side": "buy",
        "order_type": "stop_limit",
        "quantity": "2.5000",
        "price": "100.00",
        "stop_price": "99.000",
        "leverage": 2,
        "post_only": False,
        "reduce_only": False,
    }


def _nested(data: dict[str, object], section: str) -> dict[str, object]:
    """Access one known dictionary produced by a test input factory.

    Args:
        data: Factory-created nested snapshot input or decoded canonical output.
        section: Existing dictionary-valued section to inspect or alter.

    Returns:
        The requested nested dictionary without copying its values.
    """
    return cast(dict[str, object], data[section])


def _payload() -> ExecutionUnitSnapshotPayload:
    """Validate a fresh immutable snapshot for each test.

    Returns:
        A complete validated submission snapshot with exact operands.
    """
    return ExecutionUnitSnapshotPayload.model_validate(_payload_data())


def _set_observations(data: dict[str, object], instant: object) -> None:
    """Replace every instant together for exact timestamp goldens.

    Args:
        data: Mutable nested input from the payload factory.
        instant: Candidate timestamp value to apply to every observation and interpretation.
    """
    data["interpreted_at"] = instant
    for section in ("catalog", "venue_identity", "receipt"):
        _nested(data, section)["observed_at"] = instant
    _nested(data, "catalog")["spec_timestamp"] = instant


def test_canonical_payload_preserves_raw_identity_and_exact_operands() -> None:
    """Preserve economic evidence and parser identity separately.

    Given: Exact decimal operands and distinct raw and displayed asset names.
    When: The snapshot is serialized canonically.
    Then: Decimal tuples normalize while receipt text and identity spelling survive.
    """
    payload = _payload()
    canonical = canonical_snapshot_payload(payload)
    decoded = cast(dict[str, object], json.loads(canonical))
    dimensions = _nested(decoded, "dimensions")
    assert dimensions["signed_quantity"] == "25e-1"
    assert dimensions["quantity_quantum"] == "1e-3"
    assert dimensions["price"] == "1e2"
    assert dimensions["stop_price"] == "0e0"
    assert dimensions["price_quantum"] == "1e-2"
    assert dimensions["fee"] == "1e-2"
    assert dimensions["catalog_contract_multiplier"] == "1e-1"
    assert dimensions["contract_multiplier"] == "1e-1"
    assert _nested(decoded, "venue_identity")["raw_base_asset_id"] == "XXBT"
    assert _nested(decoded, "venue_identity")["display_base_asset"] == "XBT"
    assert _nested(decoded, "receipt")["material"] == _RECEIPT_TEXT
    assert _nested(decoded, "rebase")["conversion_ratio"] is None
    assert canonical == json.dumps(decoded, sort_keys=True, separators=(",", ":"))
    assert (
        snapshot_content_digest(payload)
        == hashlib.sha256(
            b"snapper:execution-unit-snapshot:v1\x00" + canonical.encode()
        ).hexdigest()
    )


def test_canonical_payload_roundtrips_without_changing_content_identity() -> None:
    """Make canonical JSON safe to persist and validate again.

    Given: A canonical snapshot containing decimal strings and integer instants.
    When: Strict JSON validation reconstructs the evidence.
    Then: Serialization and content digest remain byte-for-byte identical.
    """
    original = _payload()
    canonical = canonical_snapshot_payload(original)
    restored = ExecutionUnitSnapshotPayload.model_validate_json(canonical)
    assert canonical_snapshot_payload(restored) == canonical
    assert snapshot_content_digest(restored) == snapshot_content_digest(original)
    assert isinstance(restored.target, SubmissionSnapshotTarget)


def test_decimal_uuid_and_timezone_aliases_share_one_content_identity() -> None:
    """Canonicalize equivalent exact evidence without changing receipt text.

    Given: Equivalent decimal values, UUID spellings and aware timestamp offsets.
    When: Each input is independently validated and hashed.
    Then: Both snapshots have the same canonical bytes and content digest.
    """
    data = _payload_data()
    _nested(data, "scope")["wallet_public_id"] = "{" + _WALLET_ID.upper() + "}"
    _nested(data, "target")["command_public_id"] = "urn:uuid:" + _COMMAND_ID.upper()
    dimensions = _nested(data, "dimensions")
    dimensions.update({"signed_quantity": Decimal("25e-1"), "price": "1e2", "stop_price": "0"})
    offset = timezone(timedelta(hours=5, minutes=30))
    for section in ("catalog", "venue_identity", "receipt"):
        _nested(data, section)["observed_at"] = _OBSERVED.astimezone(offset)
    _nested(data, "catalog")["spec_timestamp"] = _OBSERVED.astimezone(offset)
    data["interpreted_at"] = _INTERPRETED.astimezone(offset)
    equivalent = ExecutionUnitSnapshotPayload.model_validate(data)
    assert canonical_snapshot_payload(equivalent) == canonical_snapshot_payload(_payload())
    assert snapshot_content_digest(equivalent) == snapshot_content_digest(_payload())
    assert equivalent.scope.wallet_public_id == _WALLET_ID
    assert equivalent.interpreted_at.tzinfo is UTC


def test_decimal_canonicalization_ignores_precision_exponents_and_rounding_traps() -> None:
    """Keep portable identity independent of ambient Decimal arithmetic settings.

    Given: Operands beyond a deliberately narrow context's precision and exponent bounds.
    When: The evidence is validated, serialized and hashed under active arithmetic traps.
    Then: Exact tuple strings and hashes survive without raising or setting context flags.
    """
    data = _payload_data()
    _nested(data, "dimensions").update(
        {
            "signed_quantity": "12345678901234567890.00000",
            "price": "-0E-900",
            "quantity_quantum": "1e-1000",
            "catalog_contract_multiplier": "1e999",
            "contract_multiplier": "1e999",
        }
    )
    expected = canonical_snapshot_payload(ExecutionUnitSnapshotPayload.model_validate(data))
    with localcontext() as context:
        context.prec = 2
        context.Emax = 9
        context.Emin = -9
        for signal in (Inexact, Rounded, Overflow, Underflow):
            context.traps[signal] = True
        context.clear_flags()
        validated = ExecutionUnitSnapshotPayload.model_validate(data)
        canonical = canonical_snapshot_payload(validated)
        digest = snapshot_content_digest(validated)
        assert not any(context.flags.values())
    decoded = cast(dict[str, object], json.loads(canonical))
    dimensions = _nested(decoded, "dimensions")
    assert dimensions["signed_quantity"] == "1234567890123456789e1"
    assert dimensions["price"] == "0e0"
    assert dimensions["quantity_quantum"] == "1e-1000"
    assert dimensions["catalog_contract_multiplier"] == "1e999"
    assert dimensions["contract_multiplier"] == "1e999"
    assert canonical == expected
    assert digest == snapshot_content_digest(ExecutionUnitSnapshotPayload.model_validate(data))


@pytest.mark.parametrize(
    ("instant", "expected"),
    (
        (datetime(1970, 1, 1, tzinfo=UTC), 0),
        (datetime(1970, 1, 1, 0, 0, 0, 1, tzinfo=UTC), 1),
        (datetime(1969, 12, 31, 23, 59, 59, 999999, tzinfo=UTC), -1),
        (datetime(9999, 12, 31, 23, 59, 59, 999999, tzinfo=UTC), 253402300799999999),
        (0, 0),
        (-1, -1),
        (253402300799999999, 253402300799999999),
    ),
)
def test_canonical_timestamps_use_exact_integer_utc_microseconds(
    instant: datetime | int, expected: int
) -> None:
    """Avoid floating-point epoch conversion even at distant dates.

    Given: An aware datetime or exact integer microsecond instant.
    When: All evidence is observed and interpreted at that instant.
    Then: Canonical JSON contains the independent integer-microsecond golden.
    """
    data = _payload_data()
    _set_observations(data, instant)
    decoded = cast(
        dict[str, object],
        json.loads(canonical_snapshot_payload(ExecutionUnitSnapshotPayload.model_validate(data))),
    )
    assert decoded["interpreted_at"] == expected
    assert _nested(decoded, "catalog")["spec_timestamp"] == expected
    assert _nested(decoded, "receipt")["observed_at"] == expected


@pytest.mark.parametrize(
    "instant",
    (
        True,
        False,
        1.25,
        Decimal("1"),
        "2026-10-22T12:30:00Z",
        datetime(2026, 10, 22),
        datetime(1, 1, 1, tzinfo=timezone(timedelta(hours=1))),
        datetime(9999, 12, 31, 23, 59, 59, 999999, tzinfo=timezone(-timedelta(hours=1))),
        10**100,
        -(10**100),
    ),
)
def test_timestamp_boundary_rejects_coercion_naive_values_and_unrepresentable_instants(
    instant: object,
) -> None:
    """Reject ambiguous or unrepresentable timestamps as validation failures.

    Given: An ambiguous timestamp or an instant whose normalization exceeds datetime bounds.
    When: Strict snapshot validation reaches the observation boundary.
    Then: The timestamp is refused without leaking arithmetic exceptions.
    """
    data = _payload_data()
    data["interpreted_at"] = instant
    with pytest.raises(ValidationError):
        ExecutionUnitSnapshotPayload.model_validate(data)


@pytest.mark.parametrize(
    ("section", "field"),
    (
        ("catalog", "spec_timestamp"),
        ("catalog", "observed_at"),
        ("venue_identity", "observed_at"),
        ("receipt", "observed_at"),
    ),
)
def test_evidence_cannot_be_observed_after_interpretation(section: str, field: str) -> None:
    """Keep each observation no later than its interpretation.

    Given: Exactly one evidence timestamp one microsecond after interpretation.
    When: The snapshot is validated.
    Then: Chronologically impossible evidence is refused.
    """
    data = _payload_data()
    _nested(data, section)[field] = _INTERPRETED + timedelta(microseconds=1)
    with pytest.raises(ValidationError, match="after its interpretation"):
        ExecutionUnitSnapshotPayload.model_validate(data)


def test_catalog_observation_may_precede_its_later_row_timestamp() -> None:
    """Keep observation time distinct from durable row persistence time.

    Given: A specification observed before its later catalog row timestamp.
    When: Both instants are no later than interpretation.
    Then: The pure payload accepts and preserves those separate instants.
    """
    data = _payload_data()
    _nested(data, "catalog")["spec_timestamp"] = _INTERPRETED
    payload = ExecutionUnitSnapshotPayload.model_validate(data)
    assert payload.catalog.observed_at == _OBSERVED
    assert payload.catalog.spec_timestamp == _INTERPRETED


@pytest.mark.parametrize(
    "field",
    (
        "signed_quantity",
        "quantity_quantum",
        "price",
        "stop_price",
        "price_quantum",
        "fee",
        "catalog_contract_multiplier",
        "contract_multiplier",
    ),
)
@pytest.mark.parametrize("value", _INVALID_DECIMALS)
def test_every_decimal_boundary_rejects_nonfinite_values_and_implicit_numbers(
    field: str, value: object
) -> None:
    """Require exact finite Decimal evidence at every operand boundary.

    Given: A non-finite lexeme or Decimal, malformed text, boolean, integer or float.
    When: It replaces an otherwise valid dimensional operand.
    Then: Strict nested validation refuses that exact field.
    """
    data = _payload_data()
    _nested(data, "dimensions")[field] = value
    with pytest.raises(ValidationError) as error:
        ExecutionUnitSnapshotPayload.model_validate(data)
    assert any(item["loc"] == ("dimensions", field) for item in error.value.errors())


@pytest.mark.parametrize(
    "field",
    ("quantity_quantum", "price_quantum", "catalog_contract_multiplier", "contract_multiplier"),
)
@pytest.mark.parametrize("value", ("0", "-0", "-1"))
def test_increments_and_contract_multiplier_must_be_strictly_positive(
    field: str, value: str
) -> None:
    """Refuse zero and negative scale operands.

    Given: An otherwise complete interpretation and a nonpositive increment or multiplier.
    When: The dimensions are validated.
    Then: The invalid positive operand is rejected before persistence.
    """
    data = _payload_data()
    _nested(data, "dimensions")[field] = value
    with pytest.raises(ValidationError, match="must be positive"):
        ExecutionUnitSnapshotPayload.model_validate(data)


@pytest.mark.parametrize(
    ("side", "quantity"),
    (("buy", "0"), ("sell", "-0"), ("buy", "-1"), ("sell", "1")),
)
def test_signed_quantity_must_be_nonzero_and_agree_with_side(side: str, quantity: str) -> None:
    """Bind signed quantity to the stated order direction.

    Given: Zero quantity or a quantity whose sign contradicts its side.
    When: The snapshot dimensions are validated.
    Then: The contradictory economic interpretation is rejected.
    """
    data = _payload_data()
    _nested(data, "dimensions").update({"side": side, "signed_quantity": quantity})
    with pytest.raises(ValidationError, match="agree with side"):
        ExecutionUnitSnapshotPayload.model_validate(data)


def test_sell_quantity_preserves_exact_negative_decimal_value() -> None:
    """Accept a proven sell without rewriting its sign or exact quantity.

    Given: A sell and an exact negative fractional contract count.
    When: The interpretation is validated and serialized.
    Then: Its canonical quantity retains the negative sign and value.
    """
    data = _payload_data()
    _nested(data, "dimensions").update({"side": "sell", "signed_quantity": "-2.5000"})
    payload = ExecutionUnitSnapshotPayload.model_validate(data)
    assert payload.dimensions.signed_quantity == Decimal("-2.5")
    assert payload.dimensions.model_dump(mode="json")["signed_quantity"] == "-25e-1"


@pytest.mark.parametrize("mask", range(1, 15))
def test_every_partial_payoff_tuple_is_rejected_atomically(mask: int) -> None:
    """Prevent partial contract metadata from implying a complete payoff model.

    Given: Any nonempty proper subset of the four nullable payoff operands.
    When: The snapshot dimensions are validated.
    Then: Every partial known tuple is refused.
    """
    data = _payload_data()
    dimensions = _nested(data, "dimensions")
    for index, field in enumerate(_PAYOFF_FIELDS):
        if mask & (1 << index):
            dimensions[field] = None
    with pytest.raises(ValidationError, match="entirely present or entirely unknown"):
        ExecutionUnitSnapshotPayload.model_validate(data)


def test_unknown_dimensions_remain_explicit_nulls_without_invented_conversion() -> None:
    """Represent missing unit knowledge without supplying a guessed convention.

    Given: No verified payoff tuple, quantity units, increments or price dimension.
    When: The unknown interpretation is validated and serialized.
    Then: Unknown values remain null and no rebase ratio appears.
    """
    data = _payload_data()
    dimensions = _nested(data, "dimensions")
    dimensions.update(dict.fromkeys(_PAYOFF_FIELDS))
    dimensions.update(
        {
            "native_order_quantity_unit": None,
            "internal_quantity_unit": None,
            "signed_quantity_unit": None,
            "quantity_quantum": None,
            "price_quantum": None,
            "price": None,
            "stop_price": None,
            "price_quantity_convention": "unknown",
            "price_currency": None,
            "catalog_contract_multiplier": None,
            "fee": None,
            "fee_currency": None,
        }
    )
    _nested(data, "rebase")["quantity_convention"] = "unknown"
    payload = ExecutionUnitSnapshotPayload.model_validate(data)
    output = payload.dimensions.model_dump(mode="json")
    assert all(output[field] is None for field in _PAYOFF_FIELDS)
    assert payload.dimensions.native_order_quantity_unit is None
    assert payload.dimensions.internal_quantity_unit is None
    assert payload.dimensions.signed_quantity_unit is None
    assert payload.dimensions.price_currency is None
    assert payload.dimensions.catalog_contract_multiplier is None
    assert payload.rebase.conversion_ratio is None


@pytest.mark.parametrize("raw_multiplier", (None, "0.2000"))
def test_unknown_payoff_preserves_independent_raw_catalog_multiplier(
    raw_multiplier: str | None,
) -> None:
    """Retain quantity-scale observations without inferring a monetary payoff tuple.

    Given: An entirely unknown payoff tuple and a separately known or unknown catalog multiplier.
    When: Snapshot dimensions are validated and serialized.
    Then: The raw scale survives independently and every monetary tuple operand stays null.
    """
    data = _payload_data()
    dimensions = _nested(data, "dimensions")
    dimensions.update(dict.fromkeys(_PAYOFF_FIELDS))
    dimensions["catalog_contract_multiplier"] = raw_multiplier
    payload = ExecutionUnitSnapshotPayload.model_validate(data)
    output = payload.dimensions.model_dump(mode="json")
    expected = None if raw_multiplier is None else Decimal(raw_multiplier)
    assert payload.dimensions.catalog_contract_multiplier == expected
    assert all(output[field] is None for field in _PAYOFF_FIELDS)
    assert output["catalog_contract_multiplier"] == (None if raw_multiplier is None else "2e-1")


@pytest.mark.parametrize("raw_multiplier", (None, "0.2"))
def test_known_payoff_multiplier_must_equal_the_observed_catalog_multiplier(
    raw_multiplier: str | None,
) -> None:
    """Require a complete interpreted payoff to agree with its actual catalog observation.

    Given: A known payoff multiplier and a different or absent raw catalog multiplier.
    When: Snapshot dimensions are validated.
    Then: The unsupported interpretation is refused rather than inventing a catalog scale.
    """
    data = _payload_data()
    _nested(data, "dimensions")["catalog_contract_multiplier"] = raw_multiplier
    with pytest.raises(ValidationError, match="observed catalog multiplier"):
        ExecutionUnitSnapshotPayload.model_validate(data)


def test_raw_catalog_multiplier_field_is_required_even_when_unknown() -> None:
    """Make the absence of historical catalog scale explicit in every snapshot.

    Given: Dimensions that omit the nullable raw catalog multiplier field entirely.
    When: Snapshot evidence is validated.
    Then: The missing observation field is rejected.
    """
    data = _payload_data()
    del _nested(data, "dimensions")["catalog_contract_multiplier"]
    with pytest.raises(ValidationError):
        ExecutionUnitSnapshotPayload.model_validate(data)


@pytest.mark.parametrize(("fee", "currency"), ((None, "USD"), ("0.1", None)))
def test_fee_amount_and_denomination_are_atomic(fee: str | None, currency: str | None) -> None:
    """Prevent undenominated fees and denominations without an amount.

    Given: Exactly one of fee amount and fee currency.
    When: The snapshot is validated.
    Then: The incomplete fee pair is rejected.
    """
    data = _payload_data()
    _nested(data, "dimensions").update({"fee": fee, "fee_currency": currency})
    with pytest.raises(ValidationError, match="supplied together"):
        ExecutionUnitSnapshotPayload.model_validate(data)


@pytest.mark.parametrize(
    ("convention", "currency"),
    (("quote_per_base", None), ("quote_per_base", "EUR"), ("unknown", "USD")),
)
def test_price_dimension_requires_verified_quote_currency(
    convention: str, currency: str | None
) -> None:
    """Prevent an unsupported currency dimension from being asserted.

    Given: A known price convention with a wrong currency or an unknown convention with one.
    When: The snapshot is validated.
    Then: The unsupported price denomination is refused.
    """
    data = _payload_data()
    _nested(data, "dimensions").update(
        {"price_quantity_convention": convention, "price_currency": currency}
    )
    with pytest.raises(ValidationError):
        ExecutionUnitSnapshotPayload.model_validate(data)


@pytest.mark.parametrize("ratio", (Decimal("1"), "1", 1, 0, True, False, {}))
def test_rebase_conversion_ratio_is_permanently_absent_in_version_one(ratio: object) -> None:
    """Refuse all claimed rebase conversions in the evidence-only schema.

    Given: Any attempted non-null conversion ratio.
    When: Version-one rebase context is validated.
    Then: The unsupported ratio is rejected regardless of scalar type.
    """
    data = _payload_data()
    _nested(data, "rebase")["conversion_ratio"] = ratio
    with pytest.raises(ValidationError):
        ExecutionUnitSnapshotPayload.model_validate(data)


@pytest.mark.parametrize(
    ("name", "value"), (("report_quantity", None), (None, "underlying_equity"))
)
def test_rebase_transport_option_name_and_value_must_be_paired(
    name: str | None, value: str | None
) -> None:
    """Keep reporting preferences tied to their actual transport option.

    Given: An option name without its value or a value without its name.
    When: The reporting context is validated.
    Then: The incomplete provenance pair is refused.
    """
    data = _payload_data()
    _nested(data, "rebase").update({"requested_option_name": name, "requested_option_value": value})
    with pytest.raises(ValidationError, match="supplied together"):
        ExecutionUnitSnapshotPayload.model_validate(data)


def test_rebase_transport_option_is_preserved_without_implying_a_ratio() -> None:
    """Record an actual reporting preference without pretending to convert quantities.

    Given: A named transport option requesting underlying-equity reporting.
    When: The context is validated.
    Then: Both original option strings survive and the conversion ratio remains null.
    """
    data = _payload_data()
    _nested(data, "rebase").update(
        {
            "quantity_convention": "underlying_equity",
            "requested_option_name": "report_quantity",
            "requested_option_value": "underlying_equity",
        }
    )
    rebase = ExecutionUnitSnapshotPayload.model_validate(data).rebase
    assert rebase.requested_option_name == "report_quantity"
    assert rebase.requested_option_value == "underlying_equity"
    assert rebase.conversion_ratio is None


@pytest.mark.parametrize(
    "section", ("target", "scope", "catalog", "dimensions", "venue_identity", "rebase", "receipt")
)
def test_unknown_fields_are_rejected_in_every_nested_evidence_model(section: str) -> None:
    """Fail closed when any nested evidence object carries an unrecognized claim.

    Given: An otherwise valid snapshot with one extra nested field.
    When: Strict payload validation runs.
    Then: The unknown field is rejected at its exact nested location.
    """
    data = _payload_data()
    _nested(data, section)["unrecognized_claim"] = "value"
    with pytest.raises(ValidationError) as error:
        ExecutionUnitSnapshotPayload.model_validate(data)
    assert any(item["type"] == "extra_forbidden" for item in error.value.errors())


@pytest.mark.parametrize(
    ("section", "field", "value"),
    (
        ("scope", "wallet_public_id", "not-a-uuid"),
        ("scope", "mode", "simulation"),
        ("scope", "instrument_public_id", 17),
        ("catalog", "version", 1),
        ("catalog", "source", " \t"),
        ("dimensions", "base_currency", {}),
        ("dimensions", "native_order_quantity_unit", "shares"),
        ("dimensions", "internal_quantity_unit", "lots"),
        ("dimensions", "signed_quantity_unit", "shares"),
        ("dimensions", "side", "long"),
        ("dimensions", "payoff_model", "quanto"),
        ("venue_identity", "raw_base_asset_id", 17),
        ("venue_identity", "native_symbol", " "),
        ("rebase", "quantity_convention", "split_adjusted"),
        ("receipt", "representation", "raw_payload"),
        ("receipt", "endpoint_or_channel", "\n"),
        ("receipt", "reported_quantity_unit", "shares"),
        ("receipt", "reported_quantity_convention", "split_adjusted"),
        ("target", "request_digest", "A" * 64),
        ("target", "request_digest", "a" * 63),
        ("target", "command_public_id", "intent-17"),
    ),
)
def test_nested_evidence_rejects_invalid_strict_fields(
    section: str, field: str, value: object
) -> None:
    """Refuse malformed identities, unsupported conventions and scalar coercion.

    Given: One invalid nested field in otherwise valid evidence.
    When: The complete payload is validated.
    Then: The unsupported claim is rejected before hashing or storage.
    """
    data = _payload_data()
    _nested(data, section)[field] = value
    with pytest.raises(ValidationError):
        ExecutionUnitSnapshotPayload.model_validate(data)


@pytest.mark.parametrize("version", (True, False, 1.0, "1", None, 2))
def test_schema_version_requires_the_exact_supported_integer(version: object) -> None:
    """Reject scalar aliases for the supported schema version.

    Given: A boolean, float, string, null or unsupported integer version.
    When: The snapshot schema discriminator is validated.
    Then: Only the exact supported integer value is eligible.
    """
    data = _payload_data()
    data["schema_version"] = version
    with pytest.raises(ValidationError):
        ExecutionUnitSnapshotPayload.model_validate(data)


@pytest.mark.parametrize("exchange", ("Kraken", "KRAKEN", " kraken", "kraken ", " "))
def test_scope_and_command_exchange_spelling_must_be_canonical(exchange: str) -> None:
    """Prevent spelling aliases from splitting request and scope identity.

    Given: An uppercase, padded or blank exchange spelling.
    When: Either a scope or a durable command request is validated.
    Then: Both boundaries reject the noncanonical spelling.
    """
    scope = _scope_data()
    scope["exchange"] = exchange
    command = _command_data()
    command["exchange"] = exchange
    with pytest.raises(ValidationError):
        SnapshotScope.model_validate(scope)
    with pytest.raises(ValidationError):
        SnapshotCommandMaterial.model_validate(command)


@pytest.mark.parametrize(
    ("representation", "material"),
    (
        ("wire_bytes_base64", ""),
        ("wire_bytes_base64", "===="),
        ("wire_bytes_base64", "Zg==\n"),
        ("wire_bytes_base64", "Zh=="),
        ("wire_bytes_base64", "Zg==="),
        ("wire_bytes_base64", "é"),
        ("decoded_json_text", "{"),
        ("decoded_json_text", "NaN"),
        ("decoded_json_text", "Infinity"),
        ("decoded_json_text", "-Infinity"),
        ("decoded_json_text", '{"nested": [NaN]}'),
    ),
)
def test_receipt_representation_rejects_noncanonical_wire_and_invalid_json(
    representation: str, material: str
) -> None:
    """Validate the claimed receipt representation without rewriting its material.

    Given: Invalid or noncanonical base64, malformed JSON or nonstandard JSON constants.
    When: Receipt evidence is validated.
    Then: The unsupported representation fails validation.
    """
    data = _receipt_data()
    data.update({"representation": representation, "material": material})
    with pytest.raises(ValidationError):
        SnapshotReceipt.model_validate(data)


@pytest.mark.parametrize("material", ("AP8=", "/w==", "Zg=="))
def test_wire_receipt_preserves_canonical_base64_for_arbitrary_bytes(material: str) -> None:
    """Retain arbitrary wire bytes without assuming UTF-8 or decoded JSON.

    Given: Nonempty canonical base64 including binary and non-UTF-8 content.
    When: A wire receipt is validated.
    Then: The exact encoded material and representation tag survive.
    """
    data = _receipt_data()
    data.update({"representation": "wire_bytes_base64", "material": material})
    receipt = SnapshotReceipt.model_validate(data)
    assert receipt.material == material
    assert receipt.representation == "wire_bytes_base64"


@pytest.mark.parametrize(
    ("name", "value"), (("report_quantity", None), (None, "underlying_equity"))
)
def test_receipt_actual_reporting_option_name_and_value_must_be_paired(
    name: str | None, value: str | None
) -> None:
    """Require observed transport options to retain both their identity and value.

    Given: A receipt reporting-option name without a value or a value without its name.
    When: The actual receipt envelope is validated.
    Then: The incomplete option evidence is refused.
    """
    data = _receipt_data()
    data.update({"reporting_option_name": name, "reporting_option_value": value})
    with pytest.raises(ValidationError, match="supplied together"):
        SnapshotReceipt.model_validate(data)


def test_decoded_receipt_whitespace_key_order_and_numeric_lexemes_are_evidence() -> None:
    """Bind exact decoded text rather than a reconstructed JSON object.

    Given: Semantically equivalent JSON objects with different original text.
    When: Their receipt and complete snapshot identities are calculated.
    Then: Each exact text survives and both identities differ.
    """
    data = _payload_data()
    alternate = '{"price":1e2,"quantity":"2.5000"}'
    _nested(data, "receipt")["material"] = alternate
    original = _payload()
    changed = ExecutionUnitSnapshotPayload.model_validate(data)
    assert json.loads(original.receipt.material) == json.loads(changed.receipt.material)
    assert original.receipt.material == _RECEIPT_TEXT
    assert changed.receipt.material == alternate
    assert receipt_content_digest(changed.receipt) != receipt_content_digest(original.receipt)
    assert snapshot_content_digest(changed) != snapshot_content_digest(original)


def test_receipt_identity_binds_tag_and_text_but_excludes_retry_provenance() -> None:
    """Give identical material a representation-sensitive portable receipt identity.

    Given: The same text valid both as base64 and JSON plus changed observation metadata.
    When: Receipt identities are calculated.
    Then: Representation changes identity while endpoint and retry time do not.
    """
    data = _receipt_data()
    data["material"] = "1234"
    decoded = SnapshotReceipt.model_validate(data)
    data["representation"] = "wire_bytes_base64"
    wire = SnapshotReceipt.model_validate(data)
    retry = wire.model_copy(
        update={"endpoint_or_channel": "private/retry", "observed_at": _INTERPRETED}
    )
    assert receipt_content_digest(decoded) != receipt_content_digest(wire)
    assert receipt_content_digest(retry) == receipt_content_digest(wire)
    assert (
        receipt_content_digest(wire)
        == hashlib.sha256(
            b"snapper:execution-unit-receipt:v1\x00wire_bytes_base64\x001234"
        ).hexdigest()
    )


def test_fill_target_binds_execution_and_venue_observation_independently() -> None:
    """Retain distinct durable identities for a fill and its source observation.

    Given: A real execution digest and a separate venue-event digest.
    When: A fill snapshot is validated and serialized.
    Then: Its discriminated target contains both identities without submission fields.
    """
    data = _payload_data()
    data["target"] = {
        "phase": "fill",
        "execution_public_id": _EXECUTION_ID,
        "execution_digest": "b" * 64,
        "venue_event_public_id": _EVENT_ID,
        "venue_event_digest": "c" * 64,
    }
    payload = ExecutionUnitSnapshotPayload.model_validate(data)
    assert isinstance(payload.target, FillSnapshotTarget)
    assert payload.target.execution_public_id == _EXECUTION_ID
    assert payload.target.venue_event_public_id == _EVENT_ID
    assert payload.target.execution_digest != payload.target.venue_event_digest
    assert "command_public_id" not in payload.target.model_dump()
    assert snapshot_content_digest(payload) != snapshot_content_digest(_payload())


@pytest.mark.parametrize("signed_unit", ("base_asset", None))
def test_submission_signed_operand_must_identify_the_native_order_unit(
    signed_unit: str | None,
) -> None:
    """Require the submission quantity operand to carry its actual native unit.

    Given: A contract-count order whose signed operand claims a different or unknown unit.
    When: Submission evidence is validated.
    Then: The contradictory or incomplete operand dimension is rejected.
    """
    data = _payload_data()
    _nested(data, "dimensions")["signed_quantity_unit"] = signed_unit
    with pytest.raises(ValidationError, match="native order unit"):
        ExecutionUnitSnapshotPayload.model_validate(data)


def test_quantity_operand_unit_is_required_even_when_explicitly_unknown() -> None:
    """Make absence of operand-dimension evidence explicit rather than implicit.

    Given: A snapshot whose dimensions omit the signed quantity unit entirely.
    When: The dimensions are validated.
    Then: Validation requires the nullable field to be supplied explicitly.
    """
    data = _payload_data()
    del _nested(data, "dimensions")["signed_quantity_unit"]
    with pytest.raises(ValidationError):
        ExecutionUnitSnapshotPayload.model_validate(data)


def test_receipt_reporting_metadata_remains_distinct_from_order_quantity_units() -> None:
    """Preserve observed fill units without assuming the native submission convention.

    Given: Native contract-count submission units and separately observed base-asset fill units.
    When: A fill snapshot records the actual receipt reporting convention.
    Then: Both distinct unit claims survive without a computed conversion ratio.
    """
    data = _payload_data()
    data["target"] = {
        "phase": "fill",
        "execution_public_id": _EXECUTION_ID,
        "execution_digest": "b" * 64,
        "venue_event_public_id": _EVENT_ID,
        "venue_event_digest": "c" * 64,
    }
    _nested(data, "dimensions")["signed_quantity_unit"] = "base_asset"
    _nested(data, "receipt").update(
        {
            "reported_quantity_unit": "base_asset",
            "reported_quantity_convention": "underlying_equity",
            "reporting_option_name": "report_quantity",
            "reporting_option_value": "underlying_equity",
        }
    )
    _nested(data, "rebase").update(
        {
            "quantity_convention": "underlying_equity",
            "requested_option_name": "report_quantity",
            "requested_option_value": "underlying_equity",
        }
    )
    payload = ExecutionUnitSnapshotPayload.model_validate(data)
    assert payload.dimensions.native_order_quantity_unit == "contract_count"
    assert payload.dimensions.signed_quantity_unit == "base_asset"
    assert payload.receipt.reported_quantity_unit == "base_asset"
    assert payload.receipt.reported_quantity_convention == "underlying_equity"
    assert payload.receipt.reporting_option_name == "report_quantity"
    assert payload.receipt.reporting_option_value == "underlying_equity"
    assert payload.rebase.conversion_ratio is None


@pytest.mark.parametrize(
    "missing",
    ("execution_public_id", "execution_digest", "venue_event_public_id", "venue_event_digest"),
)
def test_fill_target_requires_every_execution_and_venue_binding(missing: str) -> None:
    """Refuse fill evidence missing either durable source binding.

    Given: An otherwise complete fill target with one binding omitted.
    When: Strict target validation runs.
    Then: The incomplete fill target is rejected.
    """
    data: dict[str, object] = {
        "phase": "fill",
        "execution_public_id": _EXECUTION_ID,
        "execution_digest": "b" * 64,
        "venue_event_public_id": _EVENT_ID,
        "venue_event_digest": "c" * 64,
    }
    del data[missing]
    with pytest.raises(ValidationError):
        FillSnapshotTarget.model_validate(data)


@pytest.mark.parametrize(
    ("field", "value"),
    (("phase", "fill"), ("phase", "unknown"), ("execution_public_id", _EXECUTION_ID)),
)
def test_submission_target_cannot_claim_fill_identity_or_unknown_phase(
    field: str, value: str
) -> None:
    """Keep submission intent separate from execution evidence.

    Given: A submission target relabeled as a fill or carrying an execution identity.
    When: The discriminated payload is validated.
    Then: Missing, extra or unknown phase claims are refused.
    """
    data = _payload_data()
    _nested(data, "target")[field] = value
    with pytest.raises(ValidationError):
        ExecutionUnitSnapshotPayload.model_validate(data)


def test_command_identity_normalizes_exact_values_and_uses_a_separate_hash_domain() -> None:
    """Provide a portable economic request identity independent of decimal spelling.

    Given: Equivalent durable create fields with alternate decimal and UUID spellings.
    When: The requests are validated and hashed.
    Then: They share one digest under the command-specific versioned domain.
    """
    original = SnapshotCommandMaterial.model_validate(_command_data())
    data = _command_data()
    data.update(
        {
            "command_public_id": "{" + _COMMAND_ID.upper() + "}",
            "quantity": Decimal("25e-1"),
            "price": "1e2",
            "stop_price": "9.9e1",
        }
    )
    equivalent = SnapshotCommandMaterial.model_validate(data)
    canonical = json.dumps(original.model_dump(mode="json"), sort_keys=True, separators=(",", ":"))
    assert command_request_digest(equivalent) == command_request_digest(original)
    assert (
        command_request_digest(original)
        == hashlib.sha256(b"snapper:execution-unit-command:v1\x00" + canonical.encode()).hexdigest()
    )
    assert command_request_digest(original) != snapshot_content_digest(_payload())


@pytest.mark.parametrize(
    ("field", "value"),
    (
        ("command_public_id", _EXECUTION_ID),
        ("wallet_public_id", _EVENT_ID),
        ("exchange", "bybit"),
        ("mode", "live"),
        ("native_symbol", "BTCUSD"),
        ("client_order_id", "intent-18"),
        ("side", "sell"),
        ("order_type", "limit"),
        ("quantity", "2.6"),
        ("price", "100.01"),
        ("stop_price", "98.99"),
        ("leverage", 3),
        ("post_only", True),
        ("reduce_only", True),
    ),
)
def test_every_durable_command_economic_field_changes_request_identity(
    field: str, value: object
) -> None:
    """Bind all durable create-request fields to command identity.

    Given: A single changed command field and an otherwise identical request.
    When: Both requests are independently validated and hashed.
    Then: The request digest changes.
    """
    data = _command_data()
    data[field] = value
    changed = SnapshotCommandMaterial.model_validate(data)
    original = SnapshotCommandMaterial.model_validate(_command_data())
    assert command_request_digest(changed) != command_request_digest(original)


@pytest.mark.parametrize(
    ("field", "value"),
    (
        ("quantity", 2.5),
        ("quantity", True),
        ("quantity", "0"),
        ("price", "NaN"),
        ("stop_price", Decimal("sNaN")),
        ("leverage", True),
        ("leverage", 0),
        ("leverage", 2147483648),
        ("post_only", 1),
        ("reduce_only", "false"),
        ("status", "submitted"),
    ),
)
def test_command_material_rejects_coercion_invalid_leverage_and_status_fields(
    field: str, value: object
) -> None:
    """Refuse ambiguous request fields and mutable status metadata.

    Given: Invalid exact operands, scalar coercion, invalid leverage or a status field.
    When: Durable command material is validated.
    Then: The unsupported request shape is rejected.
    """
    data = _command_data()
    data[field] = value
    with pytest.raises(ValidationError):
        SnapshotCommandMaterial.model_validate(data)


@pytest.mark.parametrize(
    "section", ("target", "scope", "catalog", "dimensions", "venue_identity", "rebase", "receipt")
)
def test_snapshot_and_every_nested_record_are_frozen(section: str) -> None:
    """Prevent direct changes to an interpretation or any of its evidence records.

    Given: A validated snapshot and one of its nested immutable models.
    When: Existing fields are reassigned through ordinary attribute assignment.
    Then: Both outer and nested records reject the mutation.
    """
    payload = _payload()
    nested: BaseModel = getattr(payload, section)
    with pytest.raises(ValidationError, match="frozen"):
        payload.schema_version = 2
    field = next(iter(type(nested).model_fields))
    with pytest.raises(ValidationError, match="frozen"):
        setattr(nested, field, None)


@pytest.mark.parametrize("serialize", (canonical_snapshot_payload, snapshot_content_digest))
@pytest.mark.parametrize(
    ("section", "field", "value"),
    (
        ("scope", "exchange", "Kraken"),
        ("dimensions", "signed_quantity", True),
        ("dimensions", "contract_multiplier", None),
        ("dimensions", "catalog_contract_multiplier", None),
        ("catalog", "observed_at", _INTERPRETED + timedelta(microseconds=1)),
        ("venue_identity", "native_symbol", " "),
        ("rebase", "conversion_ratio", "1"),
        ("receipt", "material", "{"),
        ("target", "request_digest", "invalid"),
    ),
)
def test_hashing_and_canonicalization_revalidate_nested_model_copy_updates(
    serialize: Callable[[ExecutionUnitSnapshotPayload], str],
    section: str,
    field: str,
    value: object,
) -> None:
    """Revalidate frozen instances that were copied through Pydantic's unchecked path.

    Given: Invalid nested evidence introduced with model_copy rather than normal validation.
    When: Canonicalization or content hashing reaches the trust boundary.
    Then: The invalid copied value is rejected again.
    """
    payload = _payload()
    nested: BaseModel = getattr(payload, section)
    changed = nested.model_copy(update={field: value})
    tampered = payload.model_copy(update={section: changed})
    with pytest.raises(ValidationError):
        serialize(tampered)


@pytest.mark.parametrize("serialize", (canonical_snapshot_payload, snapshot_content_digest))
@pytest.mark.parametrize(
    ("field", "value"),
    (
        ("schema_version", 2),
        ("interpreted_at", _OBSERVED - timedelta(microseconds=1)),
        ("public_id", _EVENT_ID),
    ),
)
def test_payload_copy_cannot_bypass_schema_chronology_or_extra_field_validation(
    serialize: Callable[[ExecutionUnitSnapshotPayload], str], field: str, value: object
) -> None:
    """Revalidate top-level payload copies before canonical identity is trusted.

    Given: An invalid schema, impossible chronology or insertion identity copied into a payload.
    When: The payload is serialized or hashed.
    Then: The copied value is rejected rather than admitted to canonical identity.
    """
    tampered = _payload().model_copy(update={field: value})
    with pytest.raises(ValidationError):
        serialize(tampered)


def test_command_and_receipt_digest_boundaries_revalidate_copied_instances() -> None:
    """Refuse unchecked copied operands before either secondary identity is calculated.

    Given: A copied command with a boolean quantity and a copied receipt with malformed JSON.
    When: Request and receipt digest helpers receive those instances.
    Then: Both boundaries reject the invalid copies.
    """
    command = SnapshotCommandMaterial.model_validate(_command_data())
    receipt = SnapshotReceipt.model_validate(_receipt_data())
    with pytest.raises(ValidationError):
        command_request_digest(command.model_copy(update={"quantity": True}))
    with pytest.raises(ValidationError):
        receipt_content_digest(receipt.model_copy(update={"material": "{"}))


@pytest.mark.parametrize(
    ("section", "field", "value"),
    (
        ("dimensions", "signed_quantity", Decimal("NaN")),
        ("catalog", "observed_at", datetime(2026, 10, 22)),
    ),
)
def test_json_serializers_refuse_invalid_decimals_and_instants_in_unchecked_copies(
    section: str, field: str, value: object
) -> None:
    """Keep primitive serializer defenses active for unchecked copied model instances.

    Given: A copied model containing a non-finite Decimal or naive datetime.
    When: Its public JSON serializer is called directly without revalidation.
    Then: Serialization fails rather than emitting ambiguous canonical evidence.
    """
    nested: BaseModel = getattr(_payload(), section)
    tampered = nested.model_copy(update={field: value})
    with pytest.raises(PydanticSerializationError, match="Canonical"):
        tampered.model_dump(mode="json")


@pytest.mark.parametrize("sequence", (0, -1, True, "1"))
def test_write_provenance_requires_a_positive_strict_integer_sequence(sequence: object) -> None:
    """Keep local insertion provenance strict and separate from portable content.

    Given: A nonpositive or coerced local insertion sequence.
    When: Write provenance is validated.
    Then: The invalid sequence is rejected.
    """
    data: dict[str, object] = {
        "public_id": _EVENT_ID,
        "session_id": _COMMAND_ID,
        "sequence_id": sequence,
        "timestamp": _OBSERVED,
    }
    with pytest.raises(ValidationError):
        SnapshotWriteProvenance.model_validate(data)


def test_local_provenance_is_frozen_and_absent_from_portable_payload() -> None:
    """Preserve local write metadata without adding it to economic content identity.

    Given: A separate validated write-provenance record and a portable payload.
    When: The record is mutated and the payload is serialized.
    Then: Mutation fails and local insertion fields are absent from canonical content.
    """
    provenance = SnapshotWriteProvenance.model_validate(
        {
            "public_id": _EVENT_ID,
            "session_id": _COMMAND_ID,
            "sequence_id": 1,
            "timestamp": _OBSERVED,
        }
    )
    with pytest.raises(ValidationError, match="frozen"):
        provenance.sequence_id = 2
    canonical = canonical_snapshot_payload(_payload())
    assert '"session_id"' not in canonical
    assert '"sequence_id"' not in canonical
    assert '"public_id"' not in canonical
    assert provenance.timestamp.tzinfo is UTC


@pytest.mark.parametrize(
    "material",
    (
        '{"qty":"1","qty":"2"}',
        '{"payload":{"qty":"1","qty":"1"}}',
        '{"items":[{"command_public_id":"first","command_public_id":"second"}]}',
        '{"value":null,"value":0}',
        '{"a":1,"\\u0061":2}',
    ),
)
def test_decoded_receipts_reject_duplicate_keys_at_every_object_depth(material: str) -> None:
    """Decoded receipts cannot choose between conflicting or duplicated object fields.

    Given: Top-level, nested, array-contained or escaped duplicate JSON keys,
    When: Exact decoded receipt material is validated,
    Then: The representation is refused instead of silently accepting its last occurrence.
    """
    data = _receipt_data()
    data["material"] = material
    with pytest.raises(ValidationError, match="valid JSON text"):
        SnapshotReceipt.model_validate(data)


def test_identical_keys_in_distinct_json_objects_preserve_valid_receipt_material() -> None:
    """Duplicate detection is scoped to each JSON object rather than the entire document.

    Given: Distinct objects with individually unique identical field names,
    When: A decoded receipt is validated,
    Then: Every original value and its exact material representation remain available.
    """
    data = _receipt_data()
    data["material"] = '{"items":[{"qty":"1"},{"qty":"2"}],"qty":"3"}'
    receipt = SnapshotReceipt.model_validate(data)
    assert receipt.material == data["material"]
