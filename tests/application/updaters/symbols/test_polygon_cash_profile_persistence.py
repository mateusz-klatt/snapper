"""SQLite producer regressions for exact Polygon profile persistence."""

from collections.abc import Iterator
from datetime import UTC
from datetime import datetime
from decimal import Decimal
from pathlib import Path
from typing import cast

import pytest
from sqlalchemy import select
from sqlalchemy.orm import Session

from snapper.application.updaters.symbols.polygon import PolygonSymbolUpdaterService
from snapper.application.updaters.symbols.polygon_cash_profile import POLYGON_CASH_PROFILE_SOURCE
from snapper.application.updaters.symbols.types import PolygonSymbolRecord
from snapper.data.models import KNOWN_TO_MAX
from snapper.data.models import Instrument
from snapper.data.models import InstrumentSpec
from snapper.data.models import Symbol
from snapper.data.models import SymbolExchangeCapability
from snapper.data.repository import DatabaseRepository
from snapper.data.repository import InstrumentSpecInput


@pytest.fixture
def polygon_profile_catalog(
    tmp_path: Path,
) -> Iterator[tuple[PolygonSymbolUpdaterService, DatabaseRepository]]:
    """Provide an explicitly isolated SQLite catalog and its actual Polygon writer.

    Args:
        tmp_path: Pytest-owned temporary directory.

    Yields:
        Producer and repository with no provider network client instantiated.
    """
    repository = DatabaseRepository(f"sqlite:///{tmp_path / 'polygon-profile.sqlite'}")
    repository.create_all()
    updater = PolygonSymbolUpdaterService(force=True, insert_new=True)
    updater.repository = repository
    try:
        yield updater, repository
    finally:
        repository.engine.dispose()


def _stock_record() -> PolygonSymbolRecord:
    """Return an exact source reference that does not need a real provider refresh.

    Returns:
        Synthetic common stock metadata with explicit quote and provider timestamp.
    """
    return {
        "ticker": "AAPL",
        "market": "stocks",
        "type": "CS",
        "active": True,
        "currency_name": "usd",
        "last_updated_utc": "2024-01-01T12:00:00Z",
    }


def _current_spec(repository: DatabaseRepository, native_symbol: str) -> InstrumentSpec | None:
    """Read only the exact current Polygon instrument's current specification.

    Args:
        repository: Isolated test catalog.
        native_symbol: Source catalog native identifier.

    Returns:
        Current exact source specification, or None for an unclassified new source.
    """
    with repository.get_session() as session:
        assert isinstance(session, Session)
        return session.execute(
            select(InstrumentSpec)
            .join(Instrument, Instrument.public_id == InstrumentSpec.instrument_public_id)
            .join(Symbol, Symbol.public_id == Instrument.symbol_public_id)
            .where(
                Instrument.exchange == "polygon",
                Instrument.known_to == KNOWN_TO_MAX,
                Symbol.native_symbol == native_symbol,
                Symbol.known_to == KNOWN_TO_MAX,
                InstrumentSpec.known_to == KNOWN_TO_MAX,
            )
        ).scalar_one_or_none()


@pytest.mark.asyncio
@pytest.mark.parametrize(("provider_type", "kind"), [("CS", "spot"), ("ETF", "etf")])
async def test_updater_attests_exact_source_cash_kind_without_enabling_venue_trading(
    polygon_profile_catalog: tuple[PolygonSymbolUpdaterService, DatabaseRepository],
    provider_type: str,
    kind: str,
) -> None:
    """Persist source-backed cash profiles through the actual catalog updater.

    Given: An isolated catalog and an active exact stock reference of recognized type.
    When: The existing Polygon database update path processes that provider record.
    Then: The exact source gains cash provenance while real trading remains unavailable.
    """
    updater, repository = polygon_profile_catalog
    record = _stock_record()
    record["type"] = provider_type
    await updater._update_database([dict(record)])
    spec = _current_spec(repository, "AAPL")
    assert spec is not None
    assert spec.instrument_kind == kind
    assert spec.spec_source == POLYGON_CASH_PROFILE_SOURCE
    assert spec.spec_version is not None and f":{provider_type}:" in spec.spec_version
    assert spec.spec_observed_at == datetime(2024, 1, 1, 12, tzinfo=UTC)
    assert spec.quantity_unit == spec.native_order_quantity_unit == "base_asset"
    assert spec.contract_size is None
    assert spec.unit_certified is False
    with repository.get_session() as session:
        assert isinstance(session, Session)
        capability = session.execute(
            select(SymbolExchangeCapability).where(
                SymbolExchangeCapability.exchange == "polygon",
                SymbolExchangeCapability.known_to == KNOWN_TO_MAX,
            )
        ).scalar_one()
    assert capability.can_market_data is True
    assert capability.can_trade is False


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "changes",
    [
        {"type": "ETN"},
        {"type": "OPTION"},
        {"market": "indices"},
        {"active": False},
        {"last_updated_utc": "unavailable"},
        {"last_updated_utc": None},
    ],
)
async def test_later_unknown_reference_revokes_current_cash_proof_without_erasing_history(
    polygon_profile_catalog: tuple[PolygonSymbolUpdaterService, DatabaseRepository],
    changes: dict[str, object],
) -> None:
    """Use temporal replacement when a previously classified source becomes unsupported.

    Given: An exact Polygon source instrument with an earlier stock cash attestation.
    When: A later provider record becomes unknown, inactive, contradictory or incomplete.
    Then: Current proof is removed and its earlier version remains in the same UUID history.
    """
    updater, repository = polygon_profile_catalog
    original = _stock_record()
    await updater._update_database([dict(original)])
    first = _current_spec(repository, "AAPL")
    assert first is not None
    changed = cast(PolygonSymbolRecord, {**original, **changes})
    await updater._update_database([dict(changed)])
    current = _current_spec(repository, "AAPL")
    assert current is not None
    assert current.instrument_public_id == first.instrument_public_id
    assert current.instrument_kind is None
    assert current.spec_source is None
    assert current.spec_version is None
    assert current.spec_observed_at is None
    assert current.quantity_unit is None
    assert current.native_order_quantity_unit is None
    with repository.get_session() as session:
        assert isinstance(session, Session)
        historical = session.execute(
            select(InstrumentSpec).where(
                InstrumentSpec.instrument_public_id == first.instrument_public_id,
                InstrumentSpec.id == first.id,
            )
        ).scalar_one()
    assert historical.known_to != KNOWN_TO_MAX
    assert historical.instrument_kind == "spot"
    assert historical.spec_source == POLYGON_CASH_PROFILE_SOURCE
    assert historical.spec_version == first.spec_version


@pytest.mark.asyncio
async def test_cash_type_change_revises_kind_and_provenance_with_same_instrument_identity(
    polygon_profile_catalog: tuple[PolygonSymbolUpdaterService, DatabaseRepository],
) -> None:
    """Treat a provider classification change as evidence rather than a UUID rename.

    Given: A source record that previously attested the existing spot cash kind.
    When: The same exact source ticker subsequently reports recognized ETF classification.
    Then: Current kind and source content version change while the instrument UUID remains.
    """
    updater, repository = polygon_profile_catalog
    record = _stock_record()
    await updater._update_database([dict(record)])
    first = _current_spec(repository, "AAPL")
    assert first is not None
    record["type"] = "ETF"
    record["last_updated_utc"] = "2024-01-02T12:00:00Z"
    await updater._update_database([dict(record)])
    current = _current_spec(repository, "AAPL")
    assert current is not None
    assert current.instrument_public_id == first.instrument_public_id
    assert current.instrument_kind == "etf"
    assert current.spec_version != first.spec_version
    assert current.spec_observed_at == datetime(2024, 1, 2, 12, tzinfo=UTC)


@pytest.mark.asyncio
async def test_new_unknown_stock_record_does_not_create_empty_spec_rows(
    polygon_profile_catalog: tuple[PolygonSymbolUpdaterService, DatabaseRepository],
) -> None:
    """Keep absence unproven without multiplying meaningless specification rows.

    Given: A new source ticker with no provider stock classification or reference date.
    When: The ordinary updater creates its existing catalog and market data capability.
    Then: It creates no cash specification and source trading capability remains false.
    """
    updater, repository = polygon_profile_catalog
    await updater._update_database([{"ticker": "AAPL", "currency_name": "usd"}])
    assert _current_spec(repository, "AAPL") is None
    with repository.get_session() as session:
        assert isinstance(session, Session)
        assert session.execute(select(Symbol)).scalar_one().native_symbol == "AAPL"
        assert session.execute(select(SymbolExchangeCapability)).scalar_one().can_trade is False


@pytest.mark.asyncio
async def test_legacy_spot_refresh_clears_conflicting_monetary_and_funding_metadata(
    polygon_profile_catalog: tuple[PolygonSymbolUpdaterService, DatabaseRepository],
) -> None:
    """Replace an old metadata block instead of inheriting unrelated stock or contract proof.

    Given: A legacy Polygon crypto instrument with contradictory retained derivative metadata.
    When: Its ordinary spot reference is refreshed through the actual catalog updater.
    Then: Its spot kind remains and contract, provenance, margin and funding fields are cleared.
    """
    updater, repository = polygon_profile_catalog
    record: PolygonSymbolRecord = {
        "ticker": "X:BTCUSD",
        "base_currency_symbol": "BTC",
        "currency_symbol": "USD",
        "market": "crypto",
        "type": "X",
    }
    await updater._update_database([dict(record)])
    initial = _current_spec(repository, "BTC-USD")
    assert initial is not None
    with repository.get_session() as session:
        assert isinstance(session, Session)
        DatabaseRepository.revise_instrument_spec_sync(
            session=session,
            instrument_public_id=initial.instrument_public_id,
            session_id="retained-evidence-fixture",
            sequence_id=1,
            timestamp=datetime.now(UTC),
            spec=InstrumentSpecInput(
                instrument_kind="perpetual",
                quantity_unit="contract_count",
                native_order_quantity_unit="contract_count",
                contract_size=Decimal(1),
                contract_payoff_model="linear",
                contract_value_currency="BTC",
                settlement_currency="USD",
                margin_initial=0.1,
                funding_type="perpetual_funding",
                funding_frequency_hours=8,
                spec_source="retained-contract-fixture",
                spec_version="fixture-v1",
                spec_observed_at=datetime(2024, 1, 1, tzinfo=UTC),
            ),
        )
        session.commit()
    await updater._update_database([dict(record)])
    current = _current_spec(repository, "BTC-USD")
    assert current is not None
    assert current.instrument_kind == "spot"
    assert current.spec_source is None
    assert current.spec_version is None
    assert current.spec_observed_at is None
    assert current.contract_size is None
    assert current.contract_payoff_model is None
    assert current.contract_value_currency is None
    assert current.settlement_currency is None
    assert current.margin_initial is None
    assert current.funding_type is None
    assert current.funding_frequency_hours is None
    assert current.quantity_unit is None
    assert current.native_order_quantity_unit is None
