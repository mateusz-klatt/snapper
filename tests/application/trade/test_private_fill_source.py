"""Exact original fill operands and conservative refusal of ambiguous provider material."""

import base64
import json
from datetime import UTC
from datetime import datetime
from datetime import timedelta
from decimal import Decimal

import pytest

from snapper.application.trade.private_fill_source import PrivateFillSourceError
from snapper.application.trade.private_fill_source import parse_futures_private_fill
from snapper.core.contract_accounting_observation import AccountingObservationScope
from snapper.core.contract_accounting_observation import RawAccountingObservation
from tests.helpers.unit_admission import admission_id


def _record() -> dict[str, object]:
    """Represent one isolated documented native fill without source-unit inference."""
    return {
        "instrument": "PF_XBTUSD",
        "time": 1670000000123,
        "seq": 7,
        "buy": True,
        "price": 91.25,
        "qty": 2,
        "remaining_order_qty": 3,
        "order_id": "order-CaseSensitive",
        "fill_id": "fill-CaseSensitive",
        "cli_ord_id": "intent-original",
        "fill_type": "maker",
        "fee_paid": -0.125,
        "fee_currency": "USD",
    }


def _observation(wire: bytes, pointer: str | None = "/fills/0") -> RawAccountingObservation:
    """Retain actual original source bytes and a persisted selected child."""
    return RawAccountingObservation(
        scope=AccountingObservationScope(
            wallet_public_id=admission_id(10),
            exchange="kraken_futures",
            mode="live",
            environment="production",
        ),
        route_revision="actual-isolated-client-revision",
        kind="execution",
        channel="fills",
        representation="wire_bytes_base64",
        material=base64.b64encode(wire).decode("ascii"),
        target_json_pointer=pointer,
        observed_at=datetime(2026, 10, 23, tzinfo=UTC),
    )


def _frame(record: dict[str, object] | None = None) -> bytes:
    """Serialize a complete provider envelope with an explicit actual account header."""
    return json.dumps(
        {"feed": "fills", "account": "account-A", "fills": [record or _record()]}
    ).encode()


@pytest.mark.parametrize("snapshot", (False, True))
@pytest.mark.parametrize("buy", (False, True))
def test_whole_frame_selection_preserves_exact_native_operands(snapshot: bool, buy: bool) -> None:
    """A persisted selector retains one fill's exact native values and provider identities.

    Given: A complete original frame containing distinct sibling fills and a long decimal price,
    When: The parser selects the second fill from an incremental or snapshot message,
    Then: Its exact operands, direction, source time and case-sensitive identities survive.
    """
    record = {**_record(), "buy": buy}
    frame = {
        "feed": "fills_snapshot" if snapshot else "fills",
        "account": "account-A",
        "fills": [{**record, "qty": 10}, record],
    }
    wire = json.dumps(frame).replace('"price": 91.25', '"price": 91.123456789012345678901').encode()
    parsed = parse_futures_private_fill(_observation(wire, "/fills/1"))
    assert parsed.quantity == Decimal(2)
    assert parsed.price == Decimal("91.123456789012345678901")
    assert parsed.fee == Decimal("-0.125")
    assert parsed.fee_currency == "USD"
    assert parsed.provider_symbol == "PF_XBTUSD"
    assert parsed.venue_account_id == "account-A"
    assert parsed.execution_id == "fill-CaseSensitive"
    assert parsed.order_id == "order-CaseSensitive"
    assert parsed.client_order_id == "intent-original"
    assert parsed.remaining_quantity == Decimal(3)
    assert parsed.side == ("buy" if buy else "sell")
    assert parsed.occurred_at == datetime(1970, 1, 1, tzinfo=UTC) + timedelta(
        milliseconds=1670000000123
    )
    assert parsed.target_json_pointer == "/fills/1"


def test_absent_optional_provider_identities_are_never_inferred() -> None:
    """Missing optional identities remain absent instead of being inferred from another field.

    Given: An otherwise valid fill without an account header or client order identifier,
    When: The original source is interpreted,
    Then: Both optional identities remain explicitly unknown.
    """
    record = _record()
    record.pop("cli_ord_id")
    wire = json.dumps({"feed": "fills", "fills": [record]}).encode()
    parsed = parse_futures_private_fill(_observation(wire))
    assert parsed.venue_account_id is None
    assert parsed.client_order_id is None


@pytest.mark.parametrize(
    "representation", ("parsed_fields_json", "decoded_json_text", "opaque_text")
)
def test_reconstructed_material_cannot_certify_original_fill_operands(representation: str) -> None:
    """Reconstructed payloads cannot certify original execution precision.

    Given: Financial fields tagged as parsed, decoded-text or opaque fallback material,
    When: The original-fill parser receives that qualified representation,
    Then: It refuses rather than promoting reconstructed values into exact source operands.
    """
    raw = _observation(_frame())
    changed = RawAccountingObservation.model_validate(
        {**raw.model_dump(), "representation": representation, "material": _frame().decode()}
    )
    with pytest.raises(PrivateFillSourceError, match="original_wire_required"):
        parse_futures_private_fill(changed)


def test_another_venue_cannot_borrow_futures_fill_semantics() -> None:
    """A Futures-shaped payload cannot override its actual source venue.

    Given: Original fill bytes retained under an explicit Kraken Spot scope,
    When: The Futures fill parser attempts interpretation,
    Then: The venue mismatch is refused.
    """
    raw = _observation(_frame())
    changed = RawAccountingObservation.model_validate(
        {**raw.model_dump(), "scope": {**raw.scope.model_dump(), "exchange": "kraken"}}
    )
    with pytest.raises(PrivateFillSourceError, match="source_exchange_mismatch"):
        parse_futures_private_fill(changed)


@pytest.mark.parametrize("wire", (b"[]", b"{", b"\xff", b'{"price":NaN}', b'{"qty":1,"qty":2}'))
def test_invalid_or_ambiguous_json_cannot_produce_a_fill(wire: bytes) -> None:
    """Malformed or ambiguous JSON stays unavailable for financial interpretation.

    Given: Invalid UTF-8, incomplete JSON, a non-object, nonfinite data or duplicated keys,
    When: The parser decodes the retained original bytes,
    Then: It refuses the wire payload before producing a fill fact.
    """
    with pytest.raises(PrivateFillSourceError, match="wire_json_invalid"):
        parse_futures_private_fill(_observation(wire))


@pytest.mark.parametrize("feed", ("account_log", None, [], {}))
def test_other_or_malformed_channels_are_not_fill_events(feed: object) -> None:
    """Fill-looking fields do not establish a supported source channel.

    Given: A message with another feed or a malformed feed discriminator,
    When: The retained message is offered to the fill parser,
    Then: Its channel semantics remain unproven and interpretation is refused.
    """
    wire = json.dumps({"feed": feed, "fills": [_record()]}).encode()
    with pytest.raises(PrivateFillSourceError, match="feed_unsupported"):
        parse_futures_private_fill(_observation(wire))


@pytest.mark.parametrize(
    "pointer", (None, "", "/data/0", "/fills/", "/fills/00", "/fills/١", "/fills/-1", "/fills/1")
)
def test_missing_or_noncanonical_child_is_never_replaced_with_first_fill(
    pointer: str | None,
) -> None:
    """A missing or noncanonical selector cannot silently select another execution.

    Given: A one-fill frame paired with an absent, malformed or unavailable child pointer,
    When: The parser binds the requested original child,
    Then: It refuses rather than substituting the first fill or a sibling.
    """
    with pytest.raises(PrivateFillSourceError, match="selector|selected_child_missing"):
        parse_futures_private_fill(_observation(_frame(), pointer))


@pytest.mark.parametrize("fills", (None, {}, [], [None], [1]))
def test_missing_or_nonobject_selected_fill_is_retained_as_uninterpretable(fills: object) -> None:
    """An unavailable or non-object child cannot become an execution fact.

    Given: A missing, empty or malformed fills container or selected record,
    When: The parser follows the persisted child selector,
    Then: It refuses the selected structure without inventing a fill.
    """
    wire = json.dumps({"feed": "fills", "fills": fills}).encode()
    with pytest.raises(PrivateFillSourceError, match="selected_child"):
        parse_futures_private_fill(_observation(wire))


@pytest.mark.parametrize("value", (None, "", " ", 1))
def test_missing_fee_currency_is_not_assumed_to_be_usd(value: object) -> None:
    """Fee denomination requires actual nonblank source currency evidence.

    Given: A fill whose fee currency is absent, blank or not textual,
    When: Its original financial fields are parsed,
    Then: The currency is refused and no USD default is applied.
    """
    with pytest.raises(PrivateFillSourceError, match="text_unavailable:fee_currency"):
        parse_futures_private_fill(_observation(_frame({**_record(), "fee_currency": value})))


@pytest.mark.parametrize("field", ("qty", "price", "fee_paid"))
@pytest.mark.parametrize("value", (None, True, [], "not-a-number", "NaN", "Infinity"))
def test_unproven_numeric_operands_cannot_enter_accounting(field: str, value: object) -> None:
    """Invalid quantity, price or fee operands cannot acquire exact numeric evidence.

    Given: A native operand containing a boolean, missing value, malformed text or nonfinite value,
    When: The original fill is interpreted,
    Then: Numeric validation refuses the operand before producing an accounting fact.
    """
    with pytest.raises(PrivateFillSourceError, match="number_"):
        parse_futures_private_fill(_observation(_frame({**_record(), field: value})))


@pytest.mark.parametrize("value", (None, 0, -1, True, 1.5, "1"))
@pytest.mark.parametrize("field", ("seq", "time"))
def test_sequence_and_time_require_original_positive_integer_tokens(
    field: str, value: object
) -> None:
    """Source ordering and occurrence time require positive original integer tokens.

    Given: A sequence or timestamp that is absent, nonpositive, coerced or fractional,
    When: The original fill's ordering metadata is parsed,
    Then: It refuses instead of inventing a sequence or source time.
    """
    with pytest.raises(PrivateFillSourceError, match="integer_unavailable"):
        parse_futures_private_fill(_observation(_frame({**_record(), field: value})))


def test_out_of_range_venue_clock_remains_an_explicit_refusal() -> None:
    """An unrepresentable venue timestamp remains an explicit source failure.

    Given: A fill containing an overflowing original millisecond timestamp,
    When: Its occurrence time is interpreted,
    Then: The parser refuses rather than substituting the local observation clock.
    """
    with pytest.raises(PrivateFillSourceError, match="time_out_of_range"):
        parse_futures_private_fill(_observation(_frame({**_record(), "time": 10**25})))


@pytest.mark.parametrize("buy", (None, 0, 1, "buy"))
def test_direction_requires_the_actual_boolean_field(buy: object) -> None:
    """Execution direction requires the actual provider boolean discriminator.

    Given: A fill whose buy field is missing, numeric or textual,
    When: Its direction is parsed,
    Then: The parser refuses the coerced side value.
    """
    with pytest.raises(PrivateFillSourceError, match="direction_unavailable"):
        parse_futures_private_fill(_observation(_frame({**_record(), "buy": buy})))


@pytest.mark.parametrize(
    "field,value",
    (("qty", 0), ("price", -1), ("remaining_order_qty", -1), ("fill_type", "unknown")),
)
def test_impossible_economics_or_unknown_classification_is_not_a_normal_fill(
    field: str, value: object
) -> None:
    """Unsupported economics or classification cannot become an ordinary fill.

    Given: A zero quantity, negative price or remainder, or unknown fill classification,
    When: The original source is interpreted,
    Then: Validation refuses the unsupported financial semantics.
    """
    with pytest.raises(ValueError):
        parse_futures_private_fill(_observation(_frame({**_record(), field: value})))
