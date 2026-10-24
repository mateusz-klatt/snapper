"""Exact-byte parsing of product restrictions without manufacturing authenticated authority."""

import json
from datetime import UTC
from datetime import datetime
from datetime import timedelta

import pytest
from pydantic import ValidationError

from snapper.application.trade.futures_trading_instruments import FuturesProductSourceError
from snapper.application.trade.futures_trading_instruments import futures_product_requirements
from snapper.application.trade.futures_trading_instruments import parse_futures_trading_instruments


def _row(changes: dict[str, object] | None = None) -> dict[str, object]:
    """Build a closed original provider row with explicitly known account-specific flags."""
    value: dict[str, object] = {
        "symbol": "PF_XBTUSD",
        "type": "flexible_futures",
        "isExpired": False,
        "suspended": False,
        "postOnly": False,
        "restricted": False,
    }
    value.update(changes or {})
    return value


def _wire(rows: object = None, changes: dict[str, object] | None = None) -> bytes:
    """Encode exact test source bytes; this fixture performs no authenticated transport."""
    value: dict[str, object] = {
        "result": "success",
        "serverTime": "2026-10-23T18:00:00.123Z",
        "instruments": [_row()] if rows is None else rows,
    }
    value.update(changes or {})
    return json.dumps(value, separators=(",", ":")).encode()


@pytest.mark.parametrize("post_only", [False, True])
@pytest.mark.parametrize("restricted", [False, True])
def test_known_flags_report_necessary_conditions_without_identity_or_order_permission(
    post_only: bool, restricted: bool
) -> None:
    """Interpret independent exchange restrictions without claiming the account has met them.

    Given: A complete success response with explicit post-only and reduction-only conditions.
    When: The exact outgoing wire symbol is selected from the parsed source.
    Then: Both conditions remain explicit and the result contains no permission or account proof.
    """
    snapshot = parse_futures_trading_instruments(
        _wire([_row({"postOnly": post_only, "restricted": restricted})])
    )
    result = futures_product_requirements(snapshot, "PF_XBTUSD")
    assert result.model_dump(mode="json") == {
        "wire_symbol": "PF_XBTUSD",
        "instrument_type": "flexible_futures",
        "requires_post_only": post_only,
        "requires_position_reduction": restricted,
    }
    assert snapshot.server_time_text == "2026-10-23T18:00:00.123Z"
    assert snapshot.server_time == datetime(2026, 10, 23, 18, 0, 0, 123000, UTC)


@pytest.mark.parametrize("field", ["isExpired", "suspended", "postOnly", "restricted"])
@pytest.mark.parametrize("missing", [False, True])
def test_missing_and_null_flags_remain_unknown_and_cannot_become_unrestricted(
    field: str, missing: bool
) -> None:
    """Keep absent account-specific restrictions from becoming invented false values.

    Given: One selected instrument omits a required flag or explicitly supplies null.
    When: The complete original response is parsed and that exact product is selected.
    Then: Source interpretation retains uncertainty and selection refuses the unknown status.
    """
    row = _row({field: None})
    if missing:
        del row[field]
    snapshot = parse_futures_trading_instruments(_wire([row]))
    with pytest.raises(FuturesProductSourceError, match="status_unknown"):
        futures_product_requirements(snapshot, "PF_XBTUSD")


@pytest.mark.parametrize("value", [0, 1, 0.0, "false", [], {}])
def test_json_boolean_aliases_are_rejected_instead_of_coerced(value: object) -> None:
    """Require a genuine source Boolean for account restrictions.

    Given: The account restriction flag contains a numeric, textual or container alias.
    When: Original bytes are interpreted.
    Then: The parser refuses that value rather than selecting a permissive interpretation.
    """
    with pytest.raises(FuturesProductSourceError, match="boolean_required"):
        parse_futures_trading_instruments(_wire([_row({"restricted": value})]))


@pytest.mark.parametrize("field", ["isExpired", "suspended"])
def test_expired_or_suspended_market_is_not_usable_even_for_reducing_post_only_orders(
    field: str,
) -> None:
    """Keep hard market closure independent of flags that impose additional order conditions.

    Given: An explicitly expired or suspended market also reports post-only and reduction restrictions.
    When: Its exact symbol is selected.
    Then: The source refuses an accepting-market interpretation before returning any requirements.
    """
    snapshot = parse_futures_trading_instruments(
        _wire([_row({field: True, "postOnly": True, "restricted": True})])
    )
    with pytest.raises(FuturesProductSourceError, match="not_accepting_orders"):
        futures_product_requirements(snapshot, "PF_XBTUSD")


@pytest.mark.parametrize("symbol", ["PF_XBTUSD_261225", "pf_xbtusd", "PF_XBT", "PF_XBTUSD "])
def test_exchange_symbol_selection_does_not_use_case_aliases_prefixes_or_trimming(
    symbol: str,
) -> None:
    """Bind the original outgoing product identity without fuzzy normalization.

    Given: The response contains exactly PF_XBTUSD while the requested symbol differs.
    When: The selected outgoing symbol is resolved.
    Then: The lookup refuses instead of borrowing another product's restrictions.
    """
    snapshot = parse_futures_trading_instruments(_wire())
    with pytest.raises(FuturesProductSourceError, match="exact_symbol_missing"):
        futures_product_requirements(snapshot, symbol)


def test_duplicate_instrument_identity_does_not_choose_the_less_restricted_row() -> None:
    """Refuse contradictory repeated product rows across the entire original list.

    Given: Two entries share an exact symbol and disagree on the account restriction flag.
    When: Their original response bytes are parsed.
    Then: The complete response is rejected without prioritizing either row.
    """
    with pytest.raises(ValidationError, match="duplicate_wire_symbol"):
        parse_futures_trading_instruments(_wire([_row(), _row({"restricted": True})]))


@pytest.mark.parametrize(
    "wire",
    [
        b"{",
        b"[]",
        b"null",
        b"\xff",
        b'{"result":"success","result":"success"}',
        b'{"result":"success","unused":{"same":1,"same":2}}',
        b'{"result":"success","unused":NaN}',
        b'{"result":"success","unused":Infinity}',
    ],
    ids=[
        "malformed",
        "array",
        "null",
        "invalid-utf8",
        "duplicate-top",
        "duplicate-nested",
        "nan",
        "infinity",
    ],
)
def test_invalid_or_ambiguous_original_json_cannot_become_product_evidence(wire: bytes) -> None:
    """Keep duplicate keys and nonstandard numbers invalid even outside interpreted fields.

    Given: Original response bytes are malformed, ambiguous, nonobject or excessively nested.
    When: Exact source decoding runs.
    Then: It refuses with a fixed category rather than normalizing the response.
    """
    with pytest.raises(FuturesProductSourceError, match="original_json_invalid"):
        parse_futures_trading_instruments(wire)


@pytest.mark.parametrize("changes", [{"result": "error"}, {"event": "error"}, {"api_key": "x"}])
def test_failure_or_control_response_never_becomes_a_successful_market_list(
    changes: dict[str, object],
) -> None:
    """Require successful data-envelope semantics before interpreting a provider list.

    Given: A response combines product-looking rows with an error or recognized control marker.
    When: The parser inspects the whole envelope.
    Then: It refuses the source instead of selecting its otherwise shaped instruments.
    """
    with pytest.raises(FuturesProductSourceError, match="success_unproven"):
        parse_futures_trading_instruments(_wire(changes=changes))


@pytest.mark.parametrize("rows", [{}, "items", 1])
def test_nonlist_inventory_cannot_be_treated_as_a_complete_account_product_set(
    rows: object,
) -> None:
    """Require the complete documented list instead of iterating an arbitrary container.

    Given: The provider inventory field is a mapping, text or number.
    When: Its success response is interpreted.
    Then: The parser refuses incomplete inventory semantics.
    """
    with pytest.raises(FuturesProductSourceError, match="complete_list_required"):
        parse_futures_trading_instruments(_wire(rows))


@pytest.mark.parametrize("row", [[], {}, {"symbol": "", "type": "flexible_futures"}])
def test_unusable_row_identity_invalidates_the_complete_observation(row: object) -> None:
    """Keep a partly discarded inventory from establishing selected-account availability.

    Given: One instrument entry lacks a usable exact provider identity.
    When: The complete response is parsed.
    Then: It refuses the response rather than silently dropping that entry.
    """
    with pytest.raises(FuturesProductSourceError):
        parse_futures_trading_instruments(_wire([_row(), row]))


@pytest.mark.parametrize(
    "value", [None, "", "not-a-time", "2026-10-23T18:00:00", "2026-10-23T18:00:00+01:00"]
)
def test_provider_time_must_be_explicit_valid_utc_without_inventing_local_knowledge(
    value: object,
) -> None:
    """Preserve only a documented source clock while leaving local capture and freshness separate.

    Given: Server time is absent, malformed, naive or carries a non-UTC offset.
    When: A nominal success response is parsed.
    Then: It refuses instead of substituting the current local clock.
    """
    with pytest.raises(FuturesProductSourceError):
        parse_futures_trading_instruments(_wire(changes={"serverTime": value}))


def test_empty_success_list_does_not_fall_back_to_a_public_catalog() -> None:
    """Preserve an honestly empty account-specific inventory.

    Given: A successful response contains no accessible instruments.
    When: A familiar product is requested.
    Then: The snapshot remains empty and selection refuses without any external fallback.
    """
    snapshot = parse_futures_trading_instruments(_wire([]))
    assert snapshot.instruments == ()
    with pytest.raises(FuturesProductSourceError, match="exact_symbol_missing"):
        futures_product_requirements(snapshot, "PF_XBTUSD")


@pytest.mark.parametrize(
    "wire", ["{}", b"x" * (8 * 1024 * 1024 + 1)], ids=["reconstructed-text", "oversized-entity"]
)
def test_text_reconstruction_and_oversized_entities_are_refused_before_parsing(
    wire: object,
) -> None:
    """Accept only bounded original entity bytes at the source parser boundary.

    Given: The input is reconstructed text or exceeds the production entity-size limit.
    When: The parser is invoked.
    Then: It refuses the input without attempting a more permissive JSON path.
    """
    with pytest.raises(FuturesProductSourceError, match="original_entity_required"):
        parse_futures_trading_instruments(wire)


def test_unchecked_model_copy_cannot_turn_an_unknown_flag_into_a_boolean_alias() -> None:
    """Revalidate nested values when requirements are selected from an existing model instance.

    Given: A caller copies a parsed snapshot and replaces its account flag with numeric zero.
    When: The exact product is selected.
    Then: Nested strict validation rejects the altered value before interpreting restrictions.
    """
    snapshot = parse_futures_trading_instruments(_wire())
    entry = snapshot.instruments[0].model_copy(update={"restricted": 0})
    forged = snapshot.model_copy(update={"instruments": (entry,)})
    with pytest.raises(ValidationError):
        futures_product_requirements(forged, "PF_XBTUSD")


def test_copied_source_clock_must_still_match_the_original_provider_text() -> None:
    """Keep an internally inconsistent source observation from passing model-instance validation.

    Given: A copied snapshot retains provider time text but shifts the parsed time one second later.
    When: Its selected instrument requirements are interpreted.
    Then: Model revalidation rejects the contradictory clock without asserting any freshness policy.
    """
    snapshot = parse_futures_trading_instruments(_wire())
    copied = snapshot.model_copy(
        update={"server_time": snapshot.server_time + timedelta(seconds=1)}
    )
    with pytest.raises(ValidationError, match="server_time_mismatch"):
        futures_product_requirements(copied, "PF_XBTUSD")
