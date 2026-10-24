"""Prove original-wire account logs preserve native cash, source selection and precision."""

import base64
import json
from datetime import UTC
from datetime import datetime
from decimal import Decimal

import pytest
from pydantic import ValidationError

from snapper.application.portfolio.futures_account_log import FuturesAccountLogError
from snapper.application.portfolio.futures_account_log import FuturesNativeCashLeg
from snapper.application.portfolio.futures_account_log import native_futures_cash_leg
from snapper.application.portfolio.futures_account_log import parse_futures_account_log
from snapper.core.contract_accounting_observation import RawAccountingObservation


def _entry() -> dict[str, object]:
    """Provide an exact native currency leg whose components must not be double posted."""
    return {
        "id": 17,
        "date": "2026-10-23T07:01:02.000Z",
        "asset": "bch",
        "margin_account": "f-bch:usd",
        "info": "funding rate change",
        "booking_uid": "Booking-CaseSensitive",
        "old_balance": "0.01215667051",
        "new_balance": "0.01215736653",
        "collateral": "bch",
        "execution": "",
        "contract": "",
        "old_average_entry_price": 0,
        "new_average_entry_price": 0,
        "realized_pnl": "0.0000002",
        "fee": "0.0000001",
        "realized_funding": "0.00000059602",
    }


def _observation(
    document: object = None, pointer: str | None = "/new_entry"
) -> RawAccountingObservation:
    """Retain a complete private wire frame, exact selector and actual client context."""
    frame = {"feed": "account_log", "new_entry": _entry()} if document is None else document
    return _wire_observation(json.dumps(frame).encode(), pointer)


def _wire_observation(wire: bytes, pointer: str | None) -> RawAccountingObservation:
    """Keep byte-level source evidence rather than a float-parsed SDK mirror."""
    return RawAccountingObservation.model_validate(
        {
            "scope": {
                "wallet_public_id": "00000000-0000-7000-8000-000000000001",
                "exchange": "kraken_futures",
                "mode": "live",
                "environment": "demo",
            },
            "route_revision": "actual-client-selection",
            "kind": "account_cash",
            "channel": "account_log",
            "representation": "wire_bytes_base64",
            "material": base64.b64encode(wire).decode(),
            "target_json_pointer": pointer,
            "observed_at": datetime(2026, 10, 23, 8, tzinfo=UTC),
        }
    )


def test_cash_leg_posts_only_the_observed_native_balance_delta() -> None:
    """Keep fees, funding and P&L as components rather than additional cash movements.

    Given: One source currency balance leg with fee, P&L and funding metadata.
    When: Its native cash projection is produced.
    Then: Only new minus old balance moves cash in the exact source currency.
    """
    entry = parse_futures_account_log(_observation())
    leg = native_futures_cash_leg(entry)
    assert leg.native_currency == "bch"
    assert leg.cash_delta == Decimal("0.00000069602")
    assert entry.fee == Decimal("0.0000001")
    assert entry.realized_funding == Decimal("0.00000059602")
    assert entry.execution is None
    assert entry.contract is None
    assert entry.trade_price is None
    assert entry.occurred_at == datetime(2026, 10, 23, 7, 1, 2, tzinfo=UTC)


def test_original_numeric_lexemes_survive_without_a_float_round_trip() -> None:
    """Preserve sub-satoshi balance digits from the actual JSON number token.

    Given: A native wire number whose digits exceed binary float precision.
    When: The source frame is interpreted.
    Then: The exact Decimal operands and delta remain available.
    """
    wire = json.dumps({"feed": "account_log", "new_entry": _entry()}).replace(
        '"0.01215736653"', "0.0121573665300000000000000001"
    )
    entry = parse_futures_account_log(_wire_observation(wire.encode(), "/new_entry"))
    assert entry.new_balance == Decimal("0.0121573665300000000000000001")
    assert native_futures_cash_leg(entry).cash_delta == Decimal("0.0000006960200000000000000001")


def test_snapshot_children_remain_individually_bound_even_with_repeated_source_id() -> None:
    """Do not reinterpret a receipt as its sibling or collapse contradictory source facts.

    Given: Two snapshot entries claim the same venue log identity with different balances.
    When: Each independently retained receipt selects its exact child.
    Then: Their distinct source operands survive for later durable conflict detection.
    """
    other = {**_entry(), "new_balance": "0.02"}
    frame = {"feed": "account_log_snapshot", "logs": [_entry(), other]}
    first = parse_futures_account_log(_observation(frame, "/logs/0"))
    second = parse_futures_account_log(_observation(frame, "/logs/1"))
    assert first.source_log_id == second.source_log_id == 17
    assert first.target_json_pointer == "/logs/0"
    assert second.target_json_pointer == "/logs/1"
    assert first.new_balance != second.new_balance


def test_actual_frame_account_header_is_extracted_from_original_wire_evidence() -> None:
    """Preserve actual provider account identity rather than trusting callback metadata.

    Given: A genuine whole source frame includes an explicit provider account identifier.
    When: The selected native source entry is interpreted.
    Then: Its account header remains attached to the observed source operands.
    """
    frame = {"feed": "account_log", "account": "Account-Original", "new_entry": _entry()}
    entry = parse_futures_account_log(_observation(frame))
    assert entry.venue_account_id == "Account-Original"


def test_native_funding_rate_is_retained_separately_from_the_actual_cash_delta() -> None:
    """Preserve the source rate without treating it as another funding cash posting.

    Given: An actual account-log entry reports both a funding rate and native balance movement.
    When: Its source fields are parsed and cash is projected.
    Then: The exact rate survives and only new minus old balance moves native cash.
    """
    frame = {
        "feed": "account_log",
        "new_entry": {**_entry(), "funding_rate": "-8.7002552653e-8"},
    }
    entry = parse_futures_account_log(_observation(frame))
    assert entry.funding_rate == Decimal("-8.7002552653e-8")
    assert native_futures_cash_leg(entry).cash_delta == Decimal("0.00000069602")


@pytest.mark.parametrize("account", (None, "", " ", 17))
def test_present_but_invalid_account_headers_do_not_become_unknown_identity(
    account: object,
) -> None:
    """Distinguish an absent account header from a malformed header in the original source.

    Given: A retained frame claims an unusable account identity.
    When: Its native source entry is decoded.
    Then: Interpretation refuses the malformed identity instead of silently dropping it.
    """
    frame = {"feed": "account_log", "account": account, "new_entry": _entry()}
    with pytest.raises(FuturesAccountLogError, match="account_log_text_unavailable:account"):
        parse_futures_account_log(_observation(frame))


@pytest.mark.parametrize("balance", ("-1.25", "0", "0.01215667051"))
def test_cash_balances_may_be_negative_or_unchanged(balance: str) -> None:
    """Retain actual source debt and known zero movement without inventing available funds.

    Given: A legitimate native balance leg with a negative, zero or unchanged result.
    When: Cash is calculated from its exact two source balances.
    Then: The projection preserves that native result and exact arithmetic.
    """
    frame = {"feed": "account_log", "new_entry": {**_entry(), "new_balance": balance}}
    leg = native_futures_cash_leg(parse_futures_account_log(_observation(frame)))
    assert leg.entry.new_balance == Decimal(balance)
    assert leg.cash_delta == Decimal(balance) - Decimal("0.01215667051")


@pytest.mark.parametrize(
    ("updates", "reason"),
    (
        ({"asset": "pi_bchusd"}, "native_cash_currency_leg_unproven"),
        ({"collateral": None}, "native_cash_currency_leg_unproven"),
        ({"collateral": "BCH"}, "native_cash_currency_leg_unproven"),
        (
            {"asset": "flex_collateral_value", "collateral": "flex_collateral_value"},
            "collateral_valuation_is_not_native_cash",
        ),
        (
            {"asset": "cash_collateral_value", "collateral": "cash_collateral_value"},
            "collateral_valuation_is_not_native_cash",
        ),
    ),
)
def test_position_units_and_collateral_values_cannot_be_posted_as_native_cash(
    updates: dict[str, object], reason: str
) -> None:
    """Refuse unit mismatches without inferring currency from contract symbols.

    Given: A position-size or collateral-valuation log rather than a native currency leg.
    When: A caller asks to move cash from it.
    Then: Interpretation refuses and leaves the raw evidence held.
    """
    frame = {"feed": "account_log", "new_entry": {**_entry(), **updates}}
    entry = parse_futures_account_log(_observation(frame))
    with pytest.raises(FuturesAccountLogError, match=reason):
        native_futures_cash_leg(entry)


@pytest.mark.parametrize(
    ("updates", "reason"),
    (
        ({"id": True}, "account_log_source_id_invalid"),
        ({"id": "17"}, "account_log_source_id_invalid"),
        ({"id": 0}, "account_log_source_id_invalid"),
        ({"id": 1.0}, "account_log_source_id_invalid"),
        ({"asset": " "}, "account_log_text_unavailable:asset"),
        ({"contract": 3}, "account_log_text_unavailable:contract"),
        ({"old_balance": True}, "account_log_number_unavailable:old_balance"),
        ({"old_balance": None}, "account_log_number_unavailable:old_balance"),
        ({"old_balance": "garbage"}, "account_log_number_invalid:old_balance"),
        ({"old_balance": "NaN"}, "account_log_number_nonfinite:old_balance"),
        ({"old_balance": "Infinity"}, "account_log_number_nonfinite:old_balance"),
        ({"date": "yesterday"}, "account_log_source_time_invalid"),
        ({"date": "2026-10-23T07:00:00"}, "account_log_source_time_naive"),
    ),
)
def test_missing_or_ambiguous_source_fields_refuse_financial_interpretation(
    updates: dict[str, object], reason: str
) -> None:
    """Preserve raw capture while refusing absent or coerced financial source fields.

    Given: A retained source child with one invalid identity, quantity or timestamp.
    When: Financial parsing attempts to use it.
    Then: The exact safety refusal identifies the missing proof.
    """
    frame = {"feed": "account_log", "new_entry": {**_entry(), **updates}}
    with pytest.raises(FuturesAccountLogError, match=reason):
        parse_futures_account_log(_observation(frame))


@pytest.mark.parametrize(
    ("document", "pointer", "reason"),
    (
        ({"feed": "account_log", "new_entry": _entry()}, None, "child_selector_required"),
        ({"feed": "account_log", "new_entry": _entry()}, "/logs/0", "live_selector_mismatch"),
        ({"feed": "account_log_snapshot", "logs": [_entry()]}, "/logs/x", "selector_mismatch"),
        ({"feed": "account_log_snapshot", "logs": [_entry()]}, "/logs/٠", "selector_mismatch"),
        ({"feed": "account_log_snapshot", "logs": [_entry()]}, "/logs/00", "noncanonical"),
        ({"feed": "account_log_snapshot", "logs": [_entry()]}, "/logs/1", "child_missing"),
        ({"feed": "account_log_snapshot", "logs": {}}, "/logs/0", "child_missing"),
        ({"feed": "account_log", "new_entry": []}, "/new_entry", "child_object_required"),
        ({"feed": "account_log", "new_entry": None}, "/new_entry", "child_object_required"),
        ({"feed": "fills", "new_entry": _entry()}, "/new_entry", "feed_unsupported"),
        ([], "/new_entry", "wire_object_required"),
    ),
)
def test_selector_is_proved_against_the_complete_original_frame(
    document: object, pointer: str | None, reason: str
) -> None:
    """Require exact canonical child selection rather than silently taking the first event.

    Given: A complete frame with a missing, ambiguous or incorrect persisted selector.
    When: Financial interpretation chooses the child.
    Then: It refuses without assigning a different source child to the receipt.
    """
    with pytest.raises(FuturesAccountLogError, match=reason):
        parse_futures_account_log(_observation(document, pointer))


@pytest.mark.parametrize("wire", (b"{", b"\xff", b'{"x":1,"x":2}', b'{"x":NaN}'))
def test_malformed_json_is_retained_but_never_interpreted_as_money(wire: bytes) -> None:
    """Keep the strict financial decoder separate from permissive raw capture.

    Given: A durable byte receipt contains invalid JSON, duplicate keys or non-JSON numbers.
    When: The financial parser reads it.
    Then: Source interpretation refuses while the original bytes remain capturable.
    """
    observation = _wire_observation(wire, "/new_entry")
    with pytest.raises(FuturesAccountLogError, match="account_log_wire_json_invalid"):
        parse_futures_account_log(observation)


@pytest.mark.parametrize(
    "updates",
    (
        {"representation": "decoded_json_text", "material": "{}"},
        {
            "scope": {
                "wallet_public_id": "00000000-0000-7000-8000-000000000001",
                "exchange": "kraken",
                "mode": "live",
                "environment": "production",
            }
        },
    ),
)
def test_decoded_sdk_mirrors_and_other_venues_cannot_supply_source_wire_proof(
    updates: dict[str, object],
) -> None:
    """Do not upgrade float-parsed mirrors or another route into native source evidence.

    Given: A receipt originates from a different venue or already decoded SDK data.
    When: Futures native cash interpretation is requested.
    Then: The missing original source proof refuses the request.
    """
    observation = RawAccountingObservation.model_validate(
        {**_observation().model_dump(), **updates}
    )
    with pytest.raises(FuturesAccountLogError):
        parse_futures_account_log(observation)


def test_native_cash_projection_cannot_be_reconstructed_with_changed_units_or_delta() -> None:
    """Revalidate nested evidence when constructing a projected leg directly.

    Given: Valid retained source operands and a caller-supplied contradictory projection.
    When: The immutable cash DTO is validated.
    Then: It refuses both currency and balance conservation contradictions.
    """
    entry = parse_futures_account_log(_observation())
    for currency, delta in (("USD", Decimal("0.00000069602")), ("bch", Decimal(1))):
        with pytest.raises(ValidationError, match="native_cash_projection_contradicts_source"):
            FuturesNativeCashLeg(entry=entry, native_currency=currency, cash_delta=delta)


def test_inexact_decimal_subtraction_cannot_silently_round_cash() -> None:
    """Refuse an arithmetic precision limit instead of producing an approximate native balance.

    Given: Extreme source operands exceed the current exact Decimal arithmetic context.
    When: A cash delta is calculated.
    Then: The raw receipt remains held rather than accepting rounded money.
    """
    frame = {
        "feed": "account_log",
        "new_entry": {**_entry(), "old_balance": "1e100", "new_balance": "1"},
    }
    with pytest.raises(FuturesAccountLogError, match="native_cash_delta_precision_unavailable"):
        native_futures_cash_leg(parse_futures_account_log(_observation(frame)))
