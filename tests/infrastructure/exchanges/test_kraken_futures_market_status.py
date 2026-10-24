"""Current market eligibility must remain distinct from historical contract dimensions."""

from dataclasses import replace
from datetime import UTC
from datetime import datetime

import pytest
from pydantic import ValidationError

from snapper.application.updaters.symbols.base import SymbolUpdaterService
from snapper.application.updaters.symbols.kraken_futures import _instrument_metadata
from snapper.core.json_types import JsonObject
from snapper.infrastructure.exchanges.adapters.kraken_futures import parse_kraken_futures_instrument
from snapper.infrastructure.exchanges.schemas.kraken_futures import KrakenFuturesInstrumentSchema


def _instrument() -> JsonObject:
    """Supply an explicit active market observation with complete economic dimensions."""
    return {
        "symbol": "PF_XBTUSD",
        "type": "flexible_futures",
        "base": "XBT",
        "quote": "USD",
        "tickSize": 0.5,
        "contractSize": 1,
        "contractValueTradePrecision": 4,
        "tradeable": True,
        "isExpired": False,
        "suspended": False,
        "postOnly": False,
    }


@pytest.mark.parametrize(
    ("changes", "missing", "expected"),
    [
        ({}, (), "active"),
        ({"isExpired": True}, (), "inactive"),
        ({"suspended": True}, (), "inactive"),
        ({"postOnly": True}, (), "post_only"),
        ({"postOnly": True, "isExpired": True}, (), "inactive"),
        ({"postOnly": True, "suspended": True}, (), "inactive"),
        ({}, ("isExpired",), "unknown"),
        ({}, ("suspended",), "unknown"),
        ({}, ("postOnly",), "unknown"),
        ({}, ("isExpired", "suspended", "postOnly"), "unknown"),
        ({"isExpired": None}, (), "unknown"),
        ({"suspended": None}, (), "unknown"),
        ({"postOnly": None}, (), "unknown"),
        ({"postOnly": True}, ("suspended",), "unknown"),
        ({"isExpired": True}, ("suspended", "postOnly"), "inactive"),
        ({"suspended": True}, ("isExpired", "postOnly"), "inactive"),
        ({"tradeable": False}, ("isExpired", "suspended", "postOnly"), "inactive"),
    ],
)
def test_actual_market_flags_control_both_catalog_projections(
    changes: JsonObject, missing: tuple[str, ...], expected: str
) -> None:
    """Interpret current eligibility without rewriting historical contract units.

    Given: An ever-traded contract with explicit, absent or inactive market flags.
    When: The descriptor and durable metadata projections parse that observation.
    Then: Both statuses agree and historical economic dimensions remain certified.
    """
    raw = _instrument()
    raw.update(changes)
    for name in missing:
        raw.pop(name)
    schema = KrakenFuturesInstrumentSchema.model_validate(raw)
    metadata = _instrument_metadata(schema, datetime(2026, 10, 23, tzinfo=UTC))
    descriptor = parse_kraken_futures_instrument(raw)
    assert descriptor.status == metadata.status == expected
    assert metadata.unit_certified is (raw["tradeable"] is True)
    assert metadata.native_order_quantity_unit == "base_asset"
    assert metadata.contract_payoff_model == "linear"


@pytest.mark.parametrize("field", ["tradeable", "isExpired", "suspended", "postOnly"])
@pytest.mark.parametrize("value", [0, 1, "false", "true"])
def test_market_permission_flags_require_actual_json_booleans(field: str, value: int | str) -> None:
    """Reject coerced permission flags at the catalog boundary.

    Given: A status field contains a numeric or textual boolean substitute.
    When: The public catalog schema validates that source observation.
    Then: Validation refuses instead of manufacturing current eligibility.
    """
    raw = _instrument()
    raw[field] = value
    with pytest.raises(ValidationError):
        KrakenFuturesInstrumentSchema.model_validate(raw)


def test_market_status_revision_changes_provenance_without_changing_units() -> None:
    """Retain an observable market-status transition in the catalog version.

    Given: One contract transitions from active to suspended without new dimensions.
    When: Both actual source observations become durable instrument metadata.
    Then: Their provenance differs while all contract unit dimensions remain equal.
    """
    raw = _instrument()
    before = _instrument_metadata(
        KrakenFuturesInstrumentSchema.model_validate(raw), datetime(2026, 10, 23, tzinfo=UTC)
    )
    raw["suspended"] = True
    after = _instrument_metadata(
        KrakenFuturesInstrumentSchema.model_validate(raw), datetime(2026, 10, 23, tzinfo=UTC)
    )
    assert before.spec_version != after.spec_version
    assert before.unit_certified is after.unit_certified is True
    assert before.contract_size == after.contract_size
    assert before.contract_payoff_model == after.contract_payoff_model
    assert before.native_order_quantity_unit == after.native_order_quantity_unit


def test_distinct_inactive_source_flags_have_distinct_catalog_provenance() -> None:
    """Bind the actual status operands even when they produce the same derived label.

    Given: One expired observation and another suspended observation of the same contract.
    When: Both become durable catalog metadata with identical economic dimensions.
    Then: Their inactive labels match but their source interpretation digests differ.
    """
    expired = _instrument()
    expired["isExpired"] = True
    suspended = _instrument()
    suspended["suspended"] = True
    at = datetime(2026, 10, 23, tzinfo=UTC)
    first = _instrument_metadata(KrakenFuturesInstrumentSchema.model_validate(expired), at)
    second = _instrument_metadata(KrakenFuturesInstrumentSchema.model_validate(suspended), at)
    assert first.status == second.status == "inactive"
    assert first.spec_version != second.spec_version


@pytest.mark.parametrize("status", ["inactive", "unknown", "post_only"])
def test_storage_normalization_preserves_units_without_granting_market_permission(
    status: str,
) -> None:
    """Keep source-certified economic dimensions when current trading is unavailable.

    Given: Complete native futures unit evidence and a nonactive market status.
    When: The shared persistence boundary normalizes the metadata replacement.
    Then: Unit certification survives and the restrictive market status is preserved.
    """
    metadata = _instrument_metadata(
        KrakenFuturesInstrumentSchema.model_validate(_instrument()),
        datetime(2026, 10, 23, tzinfo=UTC),
    )
    normalized = SymbolUpdaterService._replacement_instrument_metadata(
        replace(metadata, status=status), "instrument"
    )
    assert normalized.unit_certified is True
    assert normalized.status == status
