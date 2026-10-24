"""Strict original-wire inventory interpretation from the retained official Futures REST schema.

The OpenOrderJson and OpenPositionJson shapes are documented by
https://docs.kraken.com/openapi/futures-rest.yaml, retained alongside this draft.
These parser tests establish no authenticated source, current inventory or
financial permission. Decimal operands remain original JSON numeric lexemes.
"""

import json
from collections.abc import Mapping
from datetime import UTC
from datetime import datetime
from datetime import timedelta
from decimal import Decimal
from decimal import localcontext
from typing import Literal
from typing import cast

import pytest
from pydantic import ValidationError

from snapper.application.trade.futures_open_inventory import FuturesInventoryTime
from snapper.application.trade.futures_open_inventory import FuturesOpenOrdersSnapshot
from snapper.application.trade.futures_open_inventory import FuturesOpenPositionsSnapshot
from snapper.application.trade.futures_open_inventory import FuturesOptionalNumber
from snapper.application.trade.futures_open_inventory import FuturesOptionalText
from snapper.application.trade.futures_open_inventory import parse_futures_open_orders
from snapper.application.trade.futures_open_inventory import parse_futures_open_positions

type _Kind = Literal["orders", "positions"]
type _Snapshot = FuturesOpenOrdersSnapshot | FuturesOpenPositionsSnapshot
_TIME = "2026-10-23T15:00:00.123456Z"
_ARRAY = {"orders": "openOrders", "positions": "openPositions"}
_ORDER: dict[str, bytes] = {
    "order_id": b'"59302619-41d2-4f0b-941f-7e7914760ad3"',
    "symbol": b'"PI_XBTUSD"',
    "side": b'"sell"',
    "orderType": b'"lmt"',
    "status": b'"untouched"',
    "filledSize": b"0",
    "reduceOnly": b"true",
    "receivedTime": b'"2019-09-05T17:01:17.410Z"',
    "lastUpdateTime": b'"2019-09-05T17:01:17.410Z"',
}
_POSITION: dict[str, bytes] = {
    "symbol": b'"FI_XBTUSD_201225"',
    "side": b'"long"',
    "size": b"20000",
    "price": b"9399.749966754434",
    "unrealizedPnl": b"1199500.664911316",
    "unrealizedFunding": b"null",
}


def _row(kind: _Kind, **changes: bytes | None) -> bytes:
    """Build original numeric JSON operands directly, without a binary-float conversion."""
    fields = dict(_ORDER if kind == "orders" else _POSITION)
    for key, value in changes.items():
        if value is None:
            fields.pop(key, None)
        else:
            fields[key] = value
    return _object(fields)


def _object(fields: Mapping[str, bytes]) -> bytes:
    """Preserve each already encoded source value, including its exact numeric lexeme."""
    return (
        b"{"
        + b",".join(json.dumps(key).encode() + b":" + value for key, value in fields.items())
        + b"}"
    )


def _wire(
    kind: _Kind, rows: tuple[bytes, ...] = (), server: str = _TIME, extra: bytes = b""
) -> bytes:
    """Wrap a literal endpoint array without adding an account identifier or completeness claim."""
    return (
        b'{"result":"success","serverTime":'
        + json.dumps(server).encode()
        + b',"'
        + _ARRAY[kind].encode()
        + b'":['
        + b",".join(rows)
        + b"]"
        + extra
        + b"}"
    )


def _parse(kind: _Kind, wire: bytes) -> _Snapshot:
    """Exercise only the actual public original-byte parser for the selected documented endpoint."""
    return (
        parse_futures_open_orders(wire) if kind == "orders" else parse_futures_open_positions(wire)
    )


@pytest.mark.parametrize("kind", ("orders", "positions"))
def test_empty_documented_response_retains_clock_without_inventing_current_flatness(
    kind: _Kind,
) -> None:
    """Given a successful empty endpoint array, when parsed, then only that response is represented.

    Given: An original success envelope with an explicit UTC server timestamp and empty array.
    When: The matching public inventory parser interprets the complete document.
    Then: It returns an empty immutable observation and the exact original timestamp text.
    """
    snapshot = _parse(kind, _wire(kind))
    assert snapshot.server_time.original_text == _TIME
    assert snapshot.server_time.instant == datetime(2026, 10, 23, 15, 0, 0, 123456, tzinfo=UTC)
    assert snapshot.model_config["frozen"]
    assert (
        snapshot.orders == ()
        if isinstance(snapshot, FuturesOpenOrdersSnapshot)
        else snapshot.positions == ()
    )
    assert not hasattr(snapshot, "account_uid") and not hasattr(snapshot, "authoritative_until")


def test_documented_order_missing_remaining_size_stays_unknown_and_keeps_literal_ids() -> None:
    """Given required OpenOrderJson fields, when optional members are absent, then no amount is inferred.

    Given: The documented first open order with its optional remaining size and prices omitted.
    When: The entire original response is interpreted.
    Then: Identity and status stay literal and each absent optional field remains explicitly unknown.
    """
    order = parse_futures_open_orders(_wire("orders", (_row("orders"),))).orders[0]
    assert order.venue_order_id == "59302619-41d2-4f0b-941f-7e7914760ad3"
    assert order.wire_symbol == "PI_XBTUSD" and order.filled_size == Decimal(0)
    assert order.status == "untouched" and order.order_type == "lmt" and order.reduce_only
    assert order.unfilled_size == FuturesOptionalNumber(present=False, value=None)
    assert order.limit_price == order.stop_price == order.unfilled_size
    assert (
        order.client_order_id
        == order.trigger_signal
        == FuturesOptionalText(present=False, value=None)
    )


def test_original_order_numeric_lexemes_and_unknown_enums_are_preserved_without_mapping() -> None:
    """Given exact JSON numerics and literal extension labels, when parsed, then no execution mapping occurs.

    Given: Original high-precision quantities, negative price, an optional client ID and unknown enum labels.
    When: Decimal context precision is deliberately smaller than the source operands.
    Then: Values retain their exact signs/digits and all source labels remain unchanged.
    """
    row = _row(
        "orders",
        filledSize=b"0.100000000000000001",
        unfilledSize=b"2.000000000000000003",
        limitPrice=b"-9.876543210987654321",
        stopPrice=b"1e256",
        cliOrdId=b'" CLIENT-ID "',
        side=b'"future-side"',
        orderType=b'"stop"',
        status=b'"future-status"',
        triggerSignal=b'"future-trigger"',
    )
    with localcontext() as context:
        context.prec = 2
        order = parse_futures_open_orders(_wire("orders", (row,))).orders[0]
    assert order.filled_size == Decimal("0.100000000000000001")
    assert order.unfilled_size.value == Decimal("2.000000000000000003")
    assert order.limit_price.value == Decimal("-9.876543210987654321")
    assert order.stop_price.value == Decimal("1e256")
    assert order.client_order_id.value == " CLIENT-ID "
    assert (order.side, order.order_type, order.status) == ("future-side", "stop", "future-status")
    assert order.trigger_signal.value == "future-trigger"


@pytest.mark.parametrize("funding", (b"null", b"-0.0073428045972263895", b"0"))
def test_documented_mature_position_preserves_signed_numerics_and_required_nullable_funding(
    funding: bytes,
) -> None:
    """Given mature positions and funding, when parsed, then maturity does not erase source inventory.

    Given: A documented unsettled mature symbol and required funding that is null or an exact number.
    When: The actual original position array is interpreted.
    Then: The position and original numeric signs remain, without a default currency or leverage regime.
    """
    row = _row("positions", size=b"-20000.000000000000000001", unrealizedFunding=funding)
    position = parse_futures_open_positions(_wire("positions", (row,))).positions[0]
    assert position.wire_symbol == "FI_XBTUSD_201225"
    assert position.size == Decimal("-20000.000000000000000001")
    assert position.average_entry_price == Decimal("9399.749966754434")
    assert position.unrealized_pnl == Decimal("1199500.664911316")
    assert position.unrealized_funding == (
        None if funding == b"null" else Decimal(funding.decode())
    )
    assert not position.pnl_currency.present and position.pnl_currency.value is None
    assert not position.max_fixed_leverage.present and position.max_fixed_leverage.value is None


@pytest.mark.parametrize("kind", ("absent", "null", "value"))
def test_position_optional_nullable_currency_and_leverage_remain_three_distinct_states(
    kind: str,
) -> None:
    """Given optional nullable position fields, when interpreted, then missing is distinct from null.

    Given: Source pnlCurrency and maxFixedLeverage are absent, explicit null, or actual values.
    When: The complete position response is parsed.
    Then: Presence and value remain separate and neither margin mode nor USD is inferred.
    """
    fields = (
        {}
        if kind == "absent"
        else {
            "pnlCurrency": b"null" if kind == "null" else b'"BTC"',
            "maxFixedLeverage": b"null" if kind == "null" else b"5.000000000000000001",
        }
    )
    position = parse_futures_open_positions(
        _wire("positions", (_row("positions", **fields),))
    ).positions[0]
    assert position.pnl_currency.present is (kind != "absent")
    assert position.max_fixed_leverage.present is (kind != "absent")
    assert position.pnl_currency.value == ("BTC" if kind == "value" else None)
    assert position.max_fixed_leverage.value == (
        Decimal("5.000000000000000001") if kind == "value" else None
    )


@pytest.mark.parametrize("field", tuple(_ORDER))
def test_every_required_order_member_is_required_for_the_whole_response(field: str) -> None:
    """Given one incomplete order, when parsing its array, then no partial inventory is returned.

    Given: One valid order followed by a row missing one documented required field.
    When: The public order parser interprets the complete response.
    Then: It refuses the response instead of silently skipping that row or manufacturing defaults.
    """
    second = (
        _row("orders", order_id=b'"022774bc-2c4a-4f26-9317-436c8d85746d"', **{field: None})
        if field != "order_id"
        else _row("orders", order_id=None)
    )
    with pytest.raises(ValueError):
        parse_futures_open_orders(_wire("orders", (_row("orders"), second)))


@pytest.mark.parametrize("field", tuple(_POSITION))
def test_every_required_position_member_includes_nullable_funding(field: str) -> None:
    """Given an incomplete position, when parsing its array, then required nullability is not absence.

    Given: A row missing one required OpenPositionJson member, including unrealizedFunding.
    When: The original position response is interpreted.
    Then: The entire response refuses rather than treating an absent funding field as explicit null.
    """
    with pytest.raises(ValueError):
        parse_futures_open_positions(_wire("positions", (_row("positions", **{field: None}),)))


@pytest.mark.parametrize(
    "field", ("unfilledSize", "limitPrice", "stopPrice", "cliOrdId", "triggerSignal")
)
def test_order_optional_members_are_not_nullable_without_documented_nullability(field: str) -> None:
    """Given an optional nonnullable order field, when null appears, then interpretation refuses.

    Given: Required order fields and one optional member explicitly set to JSON null.
    When: The original source is interpreted.
    Then: Null is refused without turning that field into absent, zero or an empty identifier.
    """
    with pytest.raises(ValueError):
        parse_futures_open_orders(_wire("orders", (_row("orders", **{field: b"null"}),)))


@pytest.mark.parametrize("numeric", (b"true", b'"1.0"', b"null", b"[]", b"{}"))
@pytest.mark.parametrize("kind", ("orders", "positions"))
def test_financial_fields_require_original_json_numbers(kind: _Kind, numeric: bytes) -> None:
    """Given a nonnumeric JSON value, when a financial field is parsed, then no coercion occurs.

    Given: A quantity replaced by Boolean, numeric text, null, array or object.
    When: The complete response is interpreted.
    Then: It refuses instead of treating that source as an exact original financial number.
    """
    field = "filledSize" if kind == "orders" else "size"
    with pytest.raises(ValueError, match="numeric_required"):
        _parse(kind, _wire(kind, (_row(kind, **{field: numeric}),)))


@pytest.mark.parametrize("invalid", (b"NaN", b"Infinity", b"-Infinity"))
@pytest.mark.parametrize("kind", ("orders", "positions"))
def test_non_json_financial_constants_are_refused_before_interpretation(
    kind: _Kind, invalid: bytes
) -> None:
    """Given a non-JSON numeric constant, when original bytes decode, then no finite observation is invented.

    Given: A response containing NaN or an infinite quantity.
    When: The actual strict original-byte decoder runs.
    Then: It returns a fixed syntax refusal and never exposes the original body in its message.
    """
    field = "filledSize" if kind == "orders" else "size"
    with pytest.raises(ValueError, match="original_json_invalid"):
        _parse(kind, _wire(kind, (_row(kind, **{field: invalid}),)))


@pytest.mark.parametrize("invalid", (b"0", b"1", b'"false"', b"null"))
def test_reduce_only_requires_an_original_boolean(invalid: bytes) -> None:
    """Given a nonboolean reduce flag, when the order is parsed, then no truthiness coercion occurs.

    Given: An otherwise complete order with a numeric, textual or null reduceOnly member.
    When: The source response is interpreted.
    Then: The actual Boolean requirement refuses the row.
    """
    with pytest.raises(ValueError, match="boolean_required"):
        parse_futures_open_orders(_wire("orders", (_row("orders", reduceOnly=invalid),)))


@pytest.mark.parametrize(
    "server", ("2026-10-23T15:00:00", "2026-10-23T15:00:00+01:00", "not-a-time")
)
@pytest.mark.parametrize("kind", ("orders", "positions"))
def test_provider_clocks_require_explicit_utc_without_inventing_local_observation_time(
    kind: _Kind, server: str
) -> None:
    """Given an invalid or non-UTC clock, when parsed, then no local time is substituted.

    Given: A source server timestamp that is naive, offset from UTC or invalid.
    When: The actual endpoint parser interprets it.
    Then: It refuses instead of replacing the source clock with current time.
    """
    with pytest.raises(ValueError, match="timestamp_"):
        _parse(kind, _wire(kind, server=server))


@pytest.mark.parametrize("field", ("receivedTime", "lastUpdateTime"))
def test_order_clocks_keep_original_explicit_utc_text(field: str) -> None:
    """Given equivalent UTC spellings, when interpreted, then original clock text remains literal.

    Given: An order timestamp written with an explicit zero UTC offset instead of Z.
    When: The actual order parser interprets it.
    Then: It preserves that original spelling while binding the correct aware instant.
    """
    text = "2019-09-05T17:01:17.410+00:00"
    order = parse_futures_open_orders(
        _wire("orders", (_row("orders", **{field: json.dumps(text).encode()}),))
    ).orders[0]
    observed = order.received_time if field == "receivedTime" else order.last_update_time
    assert observed.original_text == text and observed.instant.utcoffset() == timedelta(0)


@pytest.mark.parametrize("kind", ("orders", "positions"))
@pytest.mark.parametrize("tail", (b',"result":"success"', b',"unknown":{"x":1,"x":2}'))
def test_duplicate_json_keys_at_any_depth_refuse_original_response(
    kind: _Kind, tail: bytes
) -> None:
    """Given duplicate original keys, when decoding the complete response, then no last-key policy wins.

    Given: A duplicate envelope key or a nested unknown object with duplicate keys.
    When: Strict original-byte decoding precedes financial interpretation.
    Then: The whole response refuses with a fixed syntax message.
    """
    with pytest.raises(ValueError, match="original_json_invalid"):
        _parse(kind, _wire(kind, extra=tail))


@pytest.mark.parametrize("kind", ("orders", "positions"))
@pytest.mark.parametrize(
    "auth", ("api_key", " AUTHENT ", "nonce", "PrivateKey", "ApiSecret", "Authorization")
)
def test_all_depth_auth_names_refuse_without_copying_source_values(kind: _Kind, auth: str) -> None:
    """Given nested auth-shaped metadata, when parsed, then no financial interpretation accepts that body.

    Given: An otherwise successful response with an all-depth privacy field containing a private marker.
    When: The local strip/casefold privacy-name policy inspects the complete original document.
    Then: It returns only a fixed refusal without copying the private value into an error.
    """
    tail = b',"safe":[' + _object({auth: b'"SYNTHETIC_PRIVATE_INVENTORY_MARKER"'}) + b"]"
    with pytest.raises(ValueError, match="control_message_refused") as raised:
        _parse(kind, _wire(kind, extra=tail))
    assert "SYNTHETIC_PRIVATE_INVENTORY_MARKER" not in str(raised.value)


@pytest.mark.parametrize(
    "tail",
    (b',"method":"private/rpc"', b',"safe":{"event":" Challenge "}', b',"python-kraken-sdk":{}'),
)
@pytest.mark.parametrize("kind", ("orders", "positions"))
def test_control_discriminators_are_refused_at_every_depth(kind: _Kind, tail: bytes) -> None:
    """Given a control-shaped field, when the complete tree is inspected, then it cannot become inventory.

    Given: A private method, canonical control event or SDK control wrapper inside a success envelope.
    When: All-depth privacy classification precedes financial projection.
    Then: The source refuses with a fixed control category.
    """
    with pytest.raises(ValueError, match="control_message_refused"):
        _parse(kind, _wire(kind, extra=tail))


@pytest.mark.parametrize("kind", ("orders", "positions"))
@pytest.mark.parametrize("invalid", (b"\xff", b"{", b"[]", b"null"))
def test_invalid_utf8_json_or_root_shape_has_only_a_fixed_refusal(
    kind: _Kind, invalid: bytes
) -> None:
    """Given malformed original entity bytes, when decoding, then no body or replacement source is exposed.

    Given: Invalid UTF-8, incomplete JSON or a non-object root.
    When: The actual original-byte parser runs.
    Then: It refuses with its fixed source-syntax category.
    """
    with pytest.raises(ValueError, match="original_json_invalid"):
        _parse(kind, invalid)


@pytest.mark.parametrize("kind", ("orders", "positions"))
@pytest.mark.parametrize("tail", (b',"error":"SYNTHETIC_ERROR_MARKER"', b',"errors":[]'))
def test_success_envelope_with_error_members_is_not_accepted(kind: _Kind, tail: bytes) -> None:
    """Given an error-bearing success shape, when interpreted, then no complete success is claimed.

    Given: A successful result label accompanied by a provider error or errors member.
    When: The actual parser checks the full envelope.
    Then: It refuses without echoing the provider error text.
    """
    with pytest.raises(ValueError, match="success_unproven") as raised:
        _parse(kind, _wire(kind, extra=tail))
    assert "SYNTHETIC_ERROR_MARKER" not in str(raised.value)


@pytest.mark.parametrize("kind", ("orders", "positions"))
@pytest.mark.parametrize("value", (b"null", b"{}", b'"not-array"'))
def test_actual_inventory_member_must_be_a_complete_array(kind: _Kind, value: bytes) -> None:
    """Given a non-array endpoint member, when interpreted, then an empty inventory is never substituted.

    Given: A successful envelope whose inventory member is null, object or text.
    When: The public endpoint parser checks it.
    Then: It refuses without producing a partial or empty snapshot.
    """
    body = _object(
        {"result": b'"success"', "serverTime": json.dumps(_TIME).encode(), _ARRAY[kind]: value}
    )
    with pytest.raises(ValueError, match="complete_array_required"):
        _parse(kind, body)


@pytest.mark.parametrize("kind", ("orders", "positions"))
def test_missing_inventory_member_is_not_an_empty_response(kind: _Kind) -> None:
    """Given a missing array, when interpreted, then source absence remains a refusal.

    Given: A success envelope that omits its required endpoint inventory member.
    When: The actual parser interprets it.
    Then: It refuses instead of interpreting missing data as zero open exposure.
    """
    with pytest.raises(ValueError, match="complete_array_required"):
        _parse(kind, _object({"result": b'"success"', "serverTime": json.dumps(_TIME).encode()}))


def test_duplicate_order_uuid_spellings_refuse_without_changing_literal_order_id() -> None:
    """Given two UUID spellings of one order, when parsed, then identity cannot split the inventory.

    Given: Two complete order rows naming the same UUID with different hexadecimal letter casing.
    When: The complete array's UUID identity uniqueness is validated.
    Then: The response refuses rather than counting that order twice or choosing one spelling.
    """
    second = _row("orders", order_id=b'"59302619-41D2-4F0B-941F-7E7914760AD3"')
    with pytest.raises(ValueError, match="duplicate_order_identity"):
        parse_futures_open_orders(_wire("orders", (_row("orders"), second)))


def test_duplicate_exact_position_symbols_are_explicitly_unsupported() -> None:
    """Given repeated exact symbols, when parsed, then no undocumented position netting is invented.

    Given: Two complete positions naming the same exact provider symbol.
    When: A consumer requiring one position per symbol interprets the response.
    Then: It refuses that unsupported layout without selecting, merging or summing rows.
    """
    with pytest.raises(ValueError, match="unsupported_duplicate_positions"):
        parse_futures_open_positions(_wire("positions", (_row("positions"), _row("positions"))))


def test_invalid_order_uuid_has_a_fixed_error_without_original_provider_text() -> None:
    """Given a malformed UUID, when its actual conversion refuses, then source text cannot enter errors.

    Given: A 32-character invalid hexadecimal identity containing a synthetic private marker.
    When: The complete original order array is parsed.
    Then: A fixed UUID category replaces the library's potentially source-bearing conversion message.
    """
    marker = "SYNTHETIC_PRIVATE_UUID_XXXXXXXXX"
    assert len(marker) == 32
    with pytest.raises(ValueError, match="order_identity_invalid") as raised:
        parse_futures_open_orders(
            _wire("orders", (_row("orders", order_id=json.dumps(marker).encode()),))
        )
    assert marker not in str(raised.value)


@pytest.mark.parametrize("field", ("symbol", "side"))
@pytest.mark.parametrize("value", (b'" "', b'"\\ud800"', b"1"))
def test_financial_identifier_text_refuses_blank_surrogate_and_coerced_values(
    field: str, value: bytes
) -> None:
    """Given invalid source text, when projected, then no trimming, replacement or coercion occurs.

    Given: An otherwise valid position with blank, unpaired-surrogate or nontext symbol/side.
    When: The original row is projected.
    Then: It refuses that field rather than manufacturing an identifier.
    """
    with pytest.raises(ValueError, match="text_"):
        parse_futures_open_positions(_wire("positions", (_row("positions", **{field: value}),)))


def test_distinct_literal_position_symbols_are_not_normalized_or_collapsed() -> None:
    """Given distinct wire labels, when parsed, then no case, whitespace or alias normalization is inferred.

    Given: Complete positions with upper-case and differently spelled literal symbols.
    When: The complete array is interpreted.
    Then: Both identities remain separately observable for later supported catalog binding.
    """
    second = _row("positions", symbol=b'" fi_xbtusd_201225 "')
    snapshot = parse_futures_open_positions(_wire("positions", (_row("positions"), second)))
    assert tuple(position.wire_symbol for position in snapshot.positions) == (
        "FI_XBTUSD_201225",
        " fi_xbtusd_201225 ",
    )


@pytest.mark.parametrize("kind", ("orders", "positions"))
def test_local_entity_array_and_tree_bounds_refuse_without_truncation(kind: _Kind) -> None:
    """Given over-bound source shapes, when parsed, then local resource limits refuse the whole body.

    Given: Original entity bytes, array length, decoded node count or nesting beyond explicit local limits.
    When: The actual parser checks its versioned local bounds.
    Then: It refuses each source rather than truncating rows or claiming provider-side completeness.
    """
    with pytest.raises(ValueError, match="original_entity_required"):
        _parse(kind, b" " * (8 * 1024 * 1024 + 1))
    with pytest.raises(ValueError, match="complete_array_required"):
        _parse(kind, _wire(kind, (b"{}",) * 16385))
    with pytest.raises(ValueError, match="shape_bound"):
        _parse(kind, _wire(kind, extra=b',"safe":[' + b"0," * 65536 + b"0]"))
    with pytest.raises(ValueError, match="shape_bound"):
        _parse(kind, _wire(kind, extra=b',"safe":' + b"[" * 70 + b"0" + b"]" * 70))


@pytest.mark.parametrize("kind", ("number", "text"))
def test_public_presence_shape_cannot_attach_content_to_absence(kind: str) -> None:
    """Given an absent-field shape, when a value is attached, then the contradictory public copy refuses.

    Given: An asserted absent numeric or text field with nonnull content.
    When: The strict public observation model is constructed.
    Then: It refuses without granting source ownership from that model shape.
    """
    with pytest.raises(ValidationError, match="absent_value_supplied"):
        if kind == "number":
            FuturesOptionalNumber(present=False, value=Decimal("1"))
        else:
            FuturesOptionalText(present=False, value="USD")


def test_public_clock_copy_must_match_original_timestamp_text() -> None:
    """Given a mismatched public clock copy, when validated, then literal source time wins.

    Given: Original UTC source text and a different claimed aware instant.
    When: The strict inventory time observation is constructed.
    Then: It refuses the contradictory copy without inventing a receipt clock.
    """
    with pytest.raises(ValidationError, match="time_copy_mismatch"):
        FuturesInventoryTime(original_text=_TIME, instant=datetime(2026, 10, 23, 16, tzinfo=UTC))


@pytest.mark.parametrize("kind", ("orders", "positions"))
@pytest.mark.parametrize("result", (None, b'"error"', b"true", b'"Success"'))
def test_success_result_must_be_explicit_and_exact(kind: _Kind, result: bytes | None) -> None:
    """Given absent or nonexact success, when parsed, then no source success is invented.

    Given: An otherwise complete envelope lacking its exact documented result label.
    When: The actual parser checks the complete source.
    Then: It refuses without coercing Boolean, case or absent values into success.
    """
    fields = {"serverTime": json.dumps(_TIME).encode(), _ARRAY[kind]: b"[]"}
    if result is not None:
        fields["result"] = result
    with pytest.raises(ValueError, match="success_unproven"):
        _parse(kind, _object(fields))


@pytest.mark.parametrize("kind", ("orders", "positions"))
@pytest.mark.parametrize("row", (b"0", b"null", b"[]"))
def test_every_inventory_row_must_be_an_object(kind: _Kind, row: bytes) -> None:
    """Given a malformed array row, when the entire source is parsed, then it cannot be skipped.

    Given: An otherwise complete array containing a scalar, null or array instead of a record.
    When: The parser interprets all original rows.
    Then: It refuses that response rather than reporting an incomplete inventory.
    """
    with pytest.raises(ValueError, match="object_required"):
        _parse(kind, _wire(kind, (row,)))


@pytest.mark.parametrize("kind", ("orders", "positions"))
def test_missing_server_clock_is_not_replaced_with_current_time(kind: _Kind) -> None:
    """Given an absent provider clock, when parsed, then no local timestamp is substituted.

    Given: An otherwise successful complete empty array without serverTime.
    When: The actual parser interprets the source.
    Then: It refuses the missing timestamp rather than claiming a current observation.
    """
    with pytest.raises(ValueError, match="text_required"):
        _parse(kind, _object({"result": b'"success"', _ARRAY[kind]: b"[]"}))


@pytest.mark.parametrize("kind", ("orders", "positions"))
def test_runtime_original_entity_boundary_refuses_a_nonbytes_caller_value(kind: _Kind) -> None:
    """Given a caller-shaped source object, when parsed, then only original entity bytes are accepted.

    Given: An ordinary decoded mapping passed through an unchecked external runtime boundary.
    When: The public parser receives that object instead of original bytes.
    Then: It refuses before using a decoded replacement as original source evidence.
    """
    with pytest.raises(ValueError, match="original_entity_required"):
        _parse(kind, cast(bytes, {"result": "success"}))


def test_privacy_field_policy_does_not_normalize_or_reject_literal_financial_values() -> None:
    """Given harmless label text, when privacy keys are checked, then source values stay literal.

    Given: A client order ID containing auth vocabulary and unknown metadata with a financial event.
    When: The complete response passes the explicit privacy field-name policy.
    Then: Those values remain unchanged and do not become inferred auth fields or provider enums.
    """
    row = _row("orders", cliOrdId=b'" Authent nonce privatekey "')
    source = _wire("orders", (row,), extra=b',"metadata":{"event":"trade","label":"credentials"}')
    order = parse_futures_open_orders(source).orders[0]
    assert order.client_order_id.value == " Authent nonce privatekey "


@pytest.mark.parametrize("kind", ("orders", "positions"))
@pytest.mark.parametrize("encoding", ("utf-16-le", "utf-16-be", "utf-32-le", "utf-32-be"))
def test_bomless_wide_json_cannot_bypass_the_original_utf8_contract(
    kind: _Kind, encoding: str
) -> None:
    """Given UTF8-valid ASCII and NUL bytes, when parsed, then JSON encoding auto-detection cannot widen source.

    Given: A valid inventory response encoded as BOMless UTF16 or UTF32 rather than UTF8.
    When: Its original bytes pass ordinary UTF8 decoding but reach the strict inventory boundary.
    Then: It refuses literal NUL instead of letting json.loads(bytes) reinterpret a different encoding.
    """
    wire = _wire(kind).decode("utf-8").encode(encoding)
    assert "\x00" in wire.decode("utf-8")
    with pytest.raises(ValueError, match="original_json_invalid"):
        _parse(kind, wire)
