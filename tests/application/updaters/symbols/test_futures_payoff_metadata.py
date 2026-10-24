"""Payoff evidence from explicit venue definitions and captured public instruments.

The flexible fixtures retain the relevant fields from the unauthenticated public
instruments endpoint captured on 2026-10-22 at 21:48:27 UTC:
https://futures.kraken.com/derivatives/api/v3/instruments
Their base-quantity and USD economic settlement interpretation follows:
https://support.kraken.com/articles/4844429542676-trading-multi-collateral-derivatives
Account profit payout preferences are separate from the contract denomination.
"""

from datetime import UTC
from datetime import datetime
from decimal import Decimal

import pytest
from sqlalchemy import create_engine
from sqlalchemy import select
from sqlalchemy.orm import Session

from snapper.application.updaters.symbols.kraken_futures import KrakenFuturesSymbolUpdaterService
from snapper.application.updaters.symbols.kraken_futures import _contract_payoff_fields
from snapper.application.updaters.symbols.kraken_futures import _instrument_metadata
from snapper.application.updaters.symbols.kraken_futures import _native_order_quantity_unit
from snapper.data.models import Base
from snapper.data.models import InstrumentSpec
from snapper.data.models import SymbolExchangeCapability
from snapper.infrastructure.exchanges.futures_spec_provenance import FUTURES_SPEC_ETL_VERSION
from snapper.infrastructure.exchanges.futures_spec_provenance import FUTURES_SPEC_SOURCE
from snapper.infrastructure.exchanges.schemas.kraken_futures import KrakenFuturesInstrumentSchema

_OBSERVED_AT = datetime(2026, 10, 22, 21, 48, 27, tzinfo=UTC)
_CAPTURED: tuple[dict[str, object], ...] = (
    {
        "symbol": "PF_XBTUSD",
        "base": "BTC",
        "quote": "USD",
        "type": "flexible_futures",
        "contractSize": 1,
        "contractValueTradePrecision": 4,
        "tickSize": 1,
        "tradeable": True,
    },
    {
        "symbol": "PF_AAPLXUSD",
        "base": "AAPLx",
        "quote": "USD",
        "type": "flexible_futures",
        "contractSize": 1,
        "contractValueTradePrecision": 2,
        "tickSize": 0.01,
        "tradeable": True,
    },
    {
        "symbol": "FF_XBTUSD_261225",
        "base": "BTC",
        "quote": "USD",
        "type": "flexible_futures",
        "contractSize": 1,
        "contractValueTradePrecision": 4,
        "tickSize": 1,
        "tradeable": True,
        "lastTradingTime": "2026-12-25T08:00:00Z",
    },
)


def _schema(**overrides: object) -> KrakenFuturesInstrumentSchema:
    """Build a complete definition while allowing deliberately contradictory names."""
    payload: dict[str, object] = {
        "symbol": "opaque-product-name",
        "base": "XBT",
        "quote": "USD",
        "type": "futures_vanilla",
        "contractSize": 2.5,
        "contractValueTradePrecision": 2,
        "tickSize": 0.5,
        "tradeable": True,
    }
    payload.update(overrides)
    return KrakenFuturesInstrumentSchema.model_validate(payload)


@pytest.mark.parametrize("raw", _CAPTURED, ids=("bitcoin", "xstock", "fixed-maturity"))
def test_captured_flexible_instruments_persist_linear_economic_evidence(
    raw: dict[str, object],
) -> None:
    """Current BTC, xStock and dated definitions carry their explicit unit linear model.

    Given: Captured flexible product definitions with their published unit multiplier,
    When: The updater builds one authoritative metadata observation,
    Then: Native base-asset sizing and linear payoff evidence remain explicit and separate.
    """
    schema = KrakenFuturesInstrumentSchema.model_validate(raw)
    metadata = _instrument_metadata(schema, _OBSERVED_AT)
    assert metadata.contract_payoff_model == "linear"
    assert metadata.contract_value_currency == str(raw["base"]).upper()
    assert metadata.settlement_currency == "USD"
    assert metadata.contract_size == Decimal(1)
    assert metadata.native_order_quantity_unit == "base_asset"
    assert metadata.quantity_unit == "contract_count"
    assert metadata.unit_certified is True
    assert metadata.spec_source == FUTURES_SPEC_SOURCE
    assert metadata.spec_version is not None
    assert metadata.spec_version.startswith(f"{FUTURES_SPEC_ETL_VERSION}:")


@pytest.mark.parametrize(
    ("kind", "expected"),
    (
        ("futures_vanilla", ("linear", "BTC", "USD")),
        ("futures_inverse", ("inverse", "USD", "BTC")),
    ),
)
def test_payoff_model_and_currencies_come_from_fields_instead_of_product_name(
    kind: str,
    expected: tuple[str, str, str],
) -> None:
    """Explicit metadata normalizes currencies and dominates any symbol spelling.

    Given: Legacy source types with opaque names and explicit base and quote currencies,
    When: Native order units and payoff denominations are interpreted,
    Then: The exact source type establishes contract sizing and its own payoff model.
    """
    schema = _schema(type=kind, base=" xbt ", quote=" usd ")
    assert _contract_payoff_fields(schema) == expected
    assert _instrument_metadata(schema, _OBSERVED_AT).native_order_quantity_unit == "contract_count"


@pytest.mark.parametrize(
    ("overrides", "expected"),
    (
        ({"type": "flexible_futures", "contractSize": 1}, "base_asset"),
        ({"type": "flexible_futures", "contractSize": 2.5}, None),
        ({"type": "flexible_futures", "contractSize": 0}, None),
        ({"type": "flexible_futures", "contractSize": float("nan")}, None),
        ({"type": "flexible_futures", "contractSize": float("inf")}, None),
        ({"type": "unknown_future", "symbol": "PF_XBTUSD", "contractSize": 1}, None),
        ({"type": "options", "symbol": "PI_XBTUSD", "contractSize": 1}, None),
    ),
)
def test_native_order_quantity_units_require_a_supported_source_interpretation(
    overrides: dict[str, object], expected: str | None
) -> None:
    """Unknown products and unverified flexible multipliers never inherit contract or base units.

    Given: An explicit flexible definition or an unrecognized source product type,
    When: The native order quantity convention is interpreted,
    Then: Only the verified flexible unit multiplier establishes base-asset sizing.
    """
    schema = _schema(**overrides)
    assert _native_order_quantity_unit(schema) == expected
    metadata = _instrument_metadata(schema, _OBSERVED_AT)
    assert metadata.native_order_quantity_unit == expected
    assert metadata.unit_certified is (expected is not None)


@pytest.mark.parametrize("missing_currency", ("base", "quote"))
def test_known_contract_quantity_units_do_not_require_a_monetary_interpretation(
    missing_currency: str,
) -> None:
    """Certify known quantity conventions independently of monetary interpretation.

    Given: Complete legacy contract precision but one missing source currency,
    When: The updater separates quantity evidence from payoff evidence,
    Then: Known contract-count units remain certified while monetary fields remain unknown.
    """
    metadata = _instrument_metadata(_schema(**{missing_currency: None}), _OBSERVED_AT)

    assert metadata.native_order_quantity_unit == "contract_count"
    assert metadata.unit_certified is True
    assert metadata.contract_payoff_model is None


@pytest.mark.parametrize(
    "overrides",
    (
        {"type": "unrecognized_future"},
        {"base": None},
        {"quote": None},
        {"base": ""},
        {"quote": " "},
        {"type": "flexible_futures", "contractSize": 2.5},
        {"type": "flexible_futures", "contractSize": 0},
        {"type": "flexible_futures", "contractSize": float("nan")},
    ),
)
def test_unknown_or_incomplete_economic_interpretation_remains_atomic_null(
    overrides: dict[str, object],
) -> None:
    """Missing currency evidence and unverified flexible multipliers never get inferred.

    Given: An unknown source type, missing currency or unverified flexible multiplier,
    When: Monetary interpretation is resolved,
    Then: The entire payoff evidence tuple remains unknown.
    """
    assert _contract_payoff_fields(_schema(**overrides)) == (None, None, None)


def test_currency_interpretation_changes_the_observation_version() -> None:
    """Changing source currencies invalidates cached monetary interpretation.

    Given: Definitions differing only in an explicit source currency,
    When: Their content-version identifiers are generated,
    Then: Each distinct monetary interpretation receives a different identifier.
    """
    versions = {
        _instrument_metadata(schema, _OBSERVED_AT).spec_version
        for schema in (_schema(), _schema(base="ETH"), _schema(quote="EUR"))
    }
    assert len(versions) == 3


@pytest.mark.parametrize("raw", _CAPTURED, ids=("bitcoin", "xstock", "fixed-maturity"))
def test_real_flexible_definition_reaches_persistence_without_enabling_trading(
    raw: dict[str, object],
) -> None:
    """The actual updater writes source payoff evidence and preserves market-data-only policy.

    Given: Captured flexible definitions for crypto, tokenized exposure and a dated future,
    When: The real updater persists each definition,
    Then: Native units and payoff evidence are stored while trading remains disabled.
    """
    engine = create_engine("sqlite://")
    Base.metadata.create_all(engine)
    try:
        with Session(engine) as session:
            updater = KrakenFuturesSymbolUpdaterService()
            schema = KrakenFuturesInstrumentSchema.model_validate(raw)
            updater._persist_instrument_schema(session, schema, _OBSERVED_AT)
            session.commit()
            spec = session.execute(select(InstrumentSpec)).scalar_one()
            capability = session.execute(select(SymbolExchangeCapability)).scalar_one()
            assert spec.contract_payoff_model == "linear"
            assert spec.contract_value_currency == str(raw["base"]).upper()
            assert spec.settlement_currency == "USD"
            assert spec.contract_size == Decimal(1)
            assert spec.native_order_quantity_unit == "base_asset"
            assert spec.quantity_unit == "contract_count"
            assert capability.can_trade is False
    finally:
        engine.dispose()
