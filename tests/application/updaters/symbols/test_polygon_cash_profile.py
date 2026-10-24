"""Behavioral cases for narrow source-backed Polygon cash profiles."""

from dataclasses import replace
from datetime import UTC
from datetime import datetime
from datetime import timedelta
from typing import cast

import pytest

from snapper.application.updaters.symbols.polygon_cash_profile import POLYGON_CASH_PROFILE_SOURCE
from snapper.application.updaters.symbols.polygon_cash_profile import PolygonCashProfile
from snapper.application.updaters.symbols.polygon_cash_profile import interpret_polygon_cash_profile
from snapper.application.updaters.symbols.types import PolygonSymbolRecord

NOW = datetime(2026, 10, 23, 0, 0, tzinfo=UTC)
PROVIDER_AT = datetime(2026, 10, 20, 12, 30, tzinfo=UTC)


def _stock_record() -> PolygonSymbolRecord:
    """Return a documented stock shape with an explicit provider reference time.

    Returns:
        Fresh independent record for each test case.
    """
    return {
        "ticker": "AAPL",
        "market": "stocks",
        "type": "CS",
        "active": True,
        "currency_name": "usd",
        "last_updated_utc": "2026-10-20T12:30:00Z",
    }


def _assert_revoked(profile: PolygonCashProfile) -> None:
    """Check that a refused stock interpretation leaves no current cash proof.

    Args:
        profile: Current interpretation returned through the public function.
    """
    assert profile.instrument_kind is None
    assert profile.metadata.spec_source is None
    assert profile.metadata.spec_version is None
    assert profile.metadata.spec_observed_at is None
    assert profile.metadata.quantity_unit is None
    assert profile.metadata.native_order_quantity_unit is None
    assert profile.metadata.status is None
    assert profile.metadata.unit_certified is False


@pytest.mark.parametrize(("provider_type", "kind"), [("CS", "spot"), ("ETF", "etf")])
def test_explicit_provider_stock_type_attests_only_cash_shares(
    provider_type: str, kind: str
) -> None:
    """Bind recognized provider classification to a cash share operand.

    Given: An exact active stock source record with a provider accuracy timestamp.
    When: A common stock or ETF source is interpreted against its catalog identity.
    Then: The profile exposes base share units without derivative or margin claims.
    """
    record = _stock_record()
    record["type"] = provider_type
    profile = interpret_polygon_cash_profile(record, "AAPL", "USD", NOW)
    assert profile.instrument_kind == kind
    assert profile.metadata.quantity_unit == "base_asset"
    assert profile.metadata.native_order_quantity_unit == "base_asset"
    assert profile.metadata.spec_source == POLYGON_CASH_PROFILE_SOURCE
    assert profile.metadata.spec_version is not None
    assert profile.metadata.spec_version.startswith(f"polygon-cash-v1:{provider_type}:")
    assert len(profile.metadata.spec_version) <= 96
    assert profile.metadata.spec_observed_at == PROVIDER_AT
    assert profile.metadata.status == "active"
    assert profile.metadata.unit_certified is False
    assert profile.metadata.contract_size is None
    assert profile.metadata.contract_payoff_model is None
    assert profile.metadata.contract_value_currency is None
    assert profile.metadata.settlement_currency is None
    assert profile.metadata.margin_initial is None


@pytest.mark.parametrize(
    "change",
    [
        {"market": "indices"},
        {"market": "otc"},
        {"market": "crypto"},
        {"market": ""},
        {"type": "PFD"},
        {"type": "WARRANT"},
        {"type": "OPTION"},
        {"type": "ETN"},
        {"type": "FUTURE"},
        {"type": "cs"},
        {"active": False},
        {"currency_symbol": "$"},
        {"currency_symbol": " USD"},
        {"currency_symbol": "US"},
        {"currency_symbol": "123"},
        {"currency_symbol": "UŚD"},
        {"currency_symbol": "EUR", "currency_name": "usd"},
        {"currency_name": "US Dollar"},
        {"currency_name": "eur"},
        {"last_updated_utc": ""},
        {"last_updated_utc": "not-a-date"},
        {"last_updated_utc": "2026-10-20T12:30:00"},
        {"last_updated_utc": "2026-10-24T12:30:00Z"},
        {"last_updated_utc": "0001-01-01T00:00:00+01:00"},
    ],
)
def test_unknown_or_contradictory_source_fields_do_not_attest_stock(
    change: dict[str, object],
) -> None:
    """Fail closed on source contradictions without guessing from the ticker.

    Given: An otherwise valid stock record with one unsupported source field.
    When: The exact record is interpreted for a matching stock-looking symbol.
    Then: The resulting replacement removes all cash profile evidence.
    """
    record = cast(PolygonSymbolRecord, {**_stock_record(), **change})
    _assert_revoked(interpret_polygon_cash_profile(record, "AAPL", "USD", NOW))


@pytest.mark.parametrize("field", ["market", "type", "active", "currency_name", "last_updated_utc"])
def test_missing_provider_field_cannot_refresh_previous_attestation(field: str) -> None:
    """Refuse incomplete cached stock records without advancing their evidence.

    Given: A cached stock record that lacks one required source attribute.
    When: It is interpreted later against the same catalog identity.
    Then: It supplies an explicit unproven replacement rather than a renewed proof.
    """
    record = cast(
        PolygonSymbolRecord,
        {key: value for key, value in _stock_record().items() if key != field},
    )
    _assert_revoked(interpret_polygon_cash_profile(record, "AAPL", "USD", NOW))


@pytest.mark.parametrize(
    ("base", "quote"), [("", "USD"), ("MSFT", "USD"), ("AAPL", None), ("AAPL", "EUR")]
)
def test_source_record_requires_exact_catalog_base_and_quote(base: str, quote: str | None) -> None:
    """Reject identity mismatch instead of repairing catalog identifiers implicitly.

    Given: A valid provider record and an incompatible source catalog identity.
    When: The cash profile is interpreted for that exact instrument.
    Then: No profile or provenance is admitted for a different asset or currency.
    """
    _assert_revoked(interpret_polygon_cash_profile(_stock_record(), base, quote, NOW))


def test_existing_hyphenated_stock_mapping_is_not_silently_reidentified() -> None:
    """Leave a legacy split catalog unproven until its identity is fixed explicitly.

    Given: A provider ticker BRK-B whose existing catalog splits into BRK and B.
    When: The current source profile is considered without changing its UUID.
    Then: The inconsistent base and quote cannot acquire a share attestation.
    """
    record = _stock_record()
    record["ticker"] = "BRK-B"
    _assert_revoked(interpret_polygon_cash_profile(record, "BRK", "B", NOW))


@pytest.mark.parametrize("name", ["US Dollar", "usd"])
def test_explicit_iso_quote_field_has_documented_semantics(name: str) -> None:
    """Use the provider ISO field while retaining descriptive currency names.

    Given: An explicit lowercase USD currency_symbol and a compatible name field.
    When: The source record is interpreted against quote USD.
    Then: The profile is recognized without treating a descriptive name as a code.
    """
    record = _stock_record()
    record["currency_symbol"] = "usd"
    record["currency_name"] = name
    assert interpret_polygon_cash_profile(record, "AAPL", "USD", NOW).instrument_kind == "spot"


def test_explicit_iso_quote_does_not_require_currency_name() -> None:
    """Accept explicit source quote evidence without an optional descriptive name.

    Given: A stock source record with currency_symbol USD and no currency_name.
    When: Its profile is interpreted against the matching source catalog.
    Then: The explicit currency code is enough to identify the cash quote.
    """
    record = _stock_record()
    record["currency_symbol"] = "USD"
    del record["currency_name"]
    assert interpret_polygon_cash_profile(record, "AAPL", "USD", NOW).instrument_kind == "spot"


def test_cached_provider_time_is_not_refreshed_and_equivalent_offsets_are_normalized() -> None:
    """Preserve reference age instead of manufacturing a recent observation.

    Given: A cached provider reference accurate at a known older UTC instant.
    When: It is interpreted on two later days from an equivalent aware offset.
    Then: Both metadata records retain the provider instant and identical content version.
    """
    record = _stock_record()
    record["last_updated_utc"] = "2026-10-20T14:30:00+02:00"
    first = interpret_polygon_cash_profile(record, "AAPL", "USD", NOW)
    later = interpret_polygon_cash_profile(record, "AAPL", "USD", NOW + timedelta(days=5))
    assert first == later
    assert first.metadata.spec_observed_at == PROVIDER_AT


def test_reference_version_changes_when_source_evidence_changes_without_kind_change() -> None:
    """Bind source interpretation to content instead of only its final cash kind.

    Given: Two valid references that differ in a retained provider accuracy field.
    When: Both records produce the same stock kind and quote.
    Then: Their provenance versions remain different and neither is quantity certified.
    """
    first_record = _stock_record()
    second_record = _stock_record()
    second_record["last_updated_utc"] = "2026-10-21T12:30:00Z"
    first = interpret_polygon_cash_profile(first_record, "AAPL", "USD", NOW)
    second = interpret_polygon_cash_profile(second_record, "AAPL", "USD", NOW)
    assert first.instrument_kind == second.instrument_kind == "spot"
    assert first.metadata.spec_version != second.metadata.spec_version


@pytest.mark.parametrize(("ticker", "provider_type"), [("AAPL", "CS"), ("SPY", "ETF")])
def test_existing_provider_cache_shape_remains_unattested_without_original_accuracy_time(
    ticker: str, provider_type: str
) -> None:
    """Keep real existing cache omissions separate from synthetic positive timestamp cases.

    Given: The existing provider cache shape preserves stock classification but lacks its date.
    When: A common stock or ETF cached record is interpreted without fetching fresh evidence.
    Then: Its valid type cannot manufacture a new accuracy timestamp or cash attestation.
    """
    record: PolygonSymbolRecord = {
        "ticker": ticker,
        "market": "stocks",
        "type": provider_type,
        "active": True,
        "currency_name": "usd",
    }
    _assert_revoked(interpret_polygon_cash_profile(record, ticker, "USD", NOW))


@pytest.mark.parametrize(
    ("record", "kind"),
    [
        ({"ticker": "X:BTCUSD"}, "spot"),
        ({"ticker": "C:EURUSD"}, "spot"),
        ({"ticker": "X:BTCUSD", "market": "crypto", "type": "X"}, "spot"),
        ({"ticker": "X:BTCUSD", "market": "crypto", "type": "crypto"}, "spot"),
        ({"ticker": "C:EURUSD", "market": "fx", "type": "C"}, "spot"),
        ({"ticker": "C:EURUSD", "market": "fx", "type": "fx"}, "spot"),
        ({"ticker": "X:BTCUSD", "market": "stocks", "type": "CS"}, None),
        ({"ticker": "C:EURUSD", "market": "indices"}, None),
        ({"ticker": "X:BTCUSD", "type": "FUTURE"}, None),
        ({"ticker": "X:BTCUSD", "market": "crypto", "type": "ETF"}, None),
        ({"ticker": "I:SPX"}, None),
        ({"ticker": "AAPL"}, None),
    ],
)
def test_existing_crypto_and_fx_profiles_never_inherit_stock_metadata(
    record: PolygonSymbolRecord, kind: str | None
) -> None:
    """Keep compatible historical spot behavior separate from stock attestation.

    Given: A legacy crypto or FX source record, possibly with contradictory fields.
    When: It is interpreted through the same atomic replacement contract.
    Then: Only compatible legacy kinds remain and every stock/contract field is empty.
    """
    profile = interpret_polygon_cash_profile(record, "BTC", "USD", NOW)
    assert profile.instrument_kind == kind
    _assert_revoked(replace(profile, instrument_kind=None))
    assert profile.metadata.contract_size is None
    assert profile.metadata.contract_payoff_model is None
    assert profile.metadata.contract_value_currency is None
    assert profile.metadata.settlement_currency is None


def test_naive_interpretation_time_is_refused() -> None:
    """Reject an ambiguous local clock at the explicit interpretation boundary.

    Given: An otherwise complete provider reference and naive interpretation time.
    When: The cash profile interpreter is called.
    Then: It raises rather than comparing incompatible times or inventing a timezone.
    """
    with pytest.raises(ValueError, match="timezone aware"):
        interpret_polygon_cash_profile(_stock_record(), "AAPL", "USD", NOW.replace(tzinfo=None))
