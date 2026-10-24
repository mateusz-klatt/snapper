"""Persistence and temporal ownership of derivative monetary interpretation."""

from collections.abc import AsyncIterator
from collections.abc import Iterator
from datetime import UTC
from datetime import datetime
from datetime import timedelta
from decimal import Decimal
from typing import Literal

import pytest
from sqlalchemy import select
from sqlalchemy.exc import IntegrityError

from snapper.application.updaters.symbols.base import SymbolUpdaterService
from snapper.application.updaters.symbols.types import InstrumentMetadataInput
from snapper.data.models import InstrumentSpec
from snapper.data.repository import DatabaseRepository
from snapper.data.repository import InstrumentSpecInput
from snapper.data.repository import SQLAlchemyRepository
from snapper.infrastructure.exchanges.futures_spec_provenance import FUTURES_SPEC_ETL_VERSION
from snapper.infrastructure.exchanges.futures_spec_provenance import FUTURES_SPEC_SOURCE

type PayoffEvidence = tuple[Literal["linear", "inverse"] | None, str | None, str | None]
type NativeQuantityUnit = Literal["base_asset", "contract_count"]

_INSTRUMENT_ID = "00000000-0000-7000-8000-000000000001"
_SESSION_ID = "00000000-0000-7000-8000-000000000002"
_FIRST = datetime(2026, 10, 22, tzinfo=UTC)


@pytest.fixture
def payoff_sync_repo() -> Iterator[DatabaseRepository]:
    """Create an isolated synchronous store and close its engine after use."""
    repo = DatabaseRepository("sqlite:///:memory:")
    repo.create_all()
    try:
        yield repo
    finally:
        repo.dispose()


@pytest.fixture
async def payoff_async_repo() -> AsyncIterator[SQLAlchemyRepository]:
    """Create an isolated asynchronous store without reading configured server state."""
    repo = SQLAlchemyRepository("sqlite+aiosqlite:///:memory:")
    await repo.create_all()
    try:
        yield repo
    finally:
        await repo.engine.dispose()


def _spec(
    evidence: tuple[str | None, str | None, str | None],
    observed_at: datetime,
    native_quantity_unit: NativeQuantityUnit | None = None,
) -> InstrumentSpecInput:
    """Build quantity evidence with an independently optional monetary tuple."""
    return InstrumentSpecInput(
        instrument_kind="perpetual",
        contract_size=Decimal(1),
        quantity_unit="contract_count",
        native_order_quantity_unit=native_quantity_unit,
        spec_source=FUTURES_SPEC_SOURCE,
        spec_version=f"{FUTURES_SPEC_ETL_VERSION}:test",
        spec_observed_at=observed_at,
        unit_certified=True,
        contract_payoff_model=evidence[0],
        contract_value_currency=evidence[1],
        settlement_currency=evidence[2],
    )


def _metadata(
    evidence: PayoffEvidence,
    observed_at: datetime,
    native_quantity_unit: NativeQuantityUnit | None = None,
) -> InstrumentMetadataInput:
    """Supply complete venue precision and optional monetary evidence to the real writer."""
    return InstrumentMetadataInput(
        tick_size=0.5,
        lot_size=0.01,
        min_order_size=0.01,
        max_order_size=None,
        cost_decimals=None,
        qty_decimals=2,
        margin_initial=None,
        position_limit_long=None,
        position_limit_short=None,
        status="active",
        contract_size=Decimal(1),
        quantity_unit="contract_count",
        native_order_quantity_unit=native_quantity_unit,
        spec_source=FUTURES_SPEC_SOURCE,
        spec_version=f"{FUTURES_SPEC_ETL_VERSION}:test",
        spec_observed_at=observed_at,
        unit_certified=True,
        contract_payoff_model=evidence[0],
        contract_value_currency=evidence[1],
        settlement_currency=evidence[2],
    )


async def test_async_revisions_and_both_serializers_retain_historical_payoff(
    payoff_async_repo: SQLAlchemyRepository,
) -> None:
    """A new interpretation or explicit unknown appends a version without rewriting history.

    Given: Successive linear, inverse and unknown native-unit/payoff observations,
    When: The repository persists and projects each temporal version,
    Then: Both serializers retain the original evidence and internal reconciliation units.
    """
    observations: tuple[PayoffEvidence, ...] = (
        ("linear", "BTC", "USD"),
        ("inverse", "USD", "BTC"),
        (None, None, None),
    )
    native_units: tuple[NativeQuantityUnit | None, ...] = ("base_asset", "contract_count", None)
    for sequence, evidence in enumerate(observations, start=1):
        observed_at = _FIRST + timedelta(hours=sequence)
        result = await payoff_async_repo.revise_instrument_spec(
            instrument_public_id=_INSTRUMENT_ID,
            session_id=_SESSION_ID,
            sequence_id=sequence,
            timestamp=observed_at,
            spec=_spec(evidence, observed_at, native_units[sequence - 1]),
        )
        assert result > 0
    for sequence, expected in enumerate(observations, start=1):
        observed_at = _FIRST + timedelta(hours=sequence, seconds=1)
        row = await payoff_async_repo.get_instrument_spec(_INSTRUMENT_ID, observed_at)
        assert row is not None
        assert (
            row["contract_payoff_model"],
            row["contract_value_currency"],
            row["settlement_currency"],
        ) == expected
        assert row["unit_certified"] is True
        assert row["native_order_quantity_unit"] == native_units[sequence - 1]
        assert row["quantity_unit"] == "contract_count"
    async with payoff_async_repo.session() as session:
        rows = (
            (await session.execute(select(InstrumentSpec).order_by(InstrumentSpec.timestamp)))
            .scalars()
            .all()
        )
        assert len(rows) == 3
        for model, expected, native_unit in zip(rows, observations, native_units, strict=True):
            projected = SQLAlchemyRepository._instrument_spec_row(model)
            assert (
                projected["contract_payoff_model"],
                projected["contract_value_currency"],
                projected["settlement_currency"],
            ) == expected
            assert projected["native_order_quantity_unit"] == native_unit


def test_updater_preserves_replaces_and_clears_the_monetary_block_atomically(
    payoff_sync_repo: DatabaseRepository,
) -> None:
    """Funding-only revisions preserve evidence while explicit definitions replace all fields.

    Given: A persisted native-unit/payoff observation followed by funding-only updates,
    When: Writers preserve, replace and explicitly clear authoritative metadata,
    Then: The entire evidence block follows each revision without relabeling internal units.
    """
    metadata = _metadata(("linear", "BTC", "USD"), _FIRST, "base_asset")
    with payoff_sync_repo.get_session() as session:
        SymbolUpdaterService._revise_instrument_spec(
            session, _INSTRUMENT_ID, _FIRST, _SESSION_ID, 1, metadata=metadata
        )
        session.commit()
        SymbolUpdaterService._revise_instrument_spec(
            session,
            _INSTRUMENT_ID,
            _FIRST + timedelta(hours=1),
            _SESSION_ID,
            2,
            funding_type="perpetual_funding",
        )
        session.commit()
        preserved = DatabaseRepository.get_instrument_spec_sync(
            session, _INSTRUMENT_ID, _FIRST + timedelta(hours=1, seconds=1)
        )
        assert preserved is not None
        assert preserved.contract_payoff_model == "linear"
        assert preserved.contract_value_currency == "BTC"
        assert preserved.settlement_currency == "USD"
        assert preserved.native_order_quantity_unit == "base_asset"
        assert preserved.spec_observed_at == _FIRST
        SymbolUpdaterService._revise_instrument_spec(
            session,
            _INSTRUMENT_ID,
            _FIRST + timedelta(hours=2),
            _SESSION_ID,
            3,
            metadata=_metadata(
                ("inverse", "USD", "BTC"), _FIRST + timedelta(hours=2), "contract_count"
            ),
        )
        session.commit()
        SymbolUpdaterService._revise_instrument_spec(
            session,
            _INSTRUMENT_ID,
            _FIRST + timedelta(hours=3),
            _SESSION_ID,
            4,
            metadata=_metadata((None, None, None), _FIRST + timedelta(hours=3)),
        )
        session.commit()
        rows = (
            session.execute(select(InstrumentSpec).order_by(InstrumentSpec.timestamp))
            .scalars()
            .all()
        )
        assert [
            (
                row.contract_payoff_model,
                row.contract_value_currency,
                row.settlement_currency,
                row.native_order_quantity_unit,
            )
            for row in rows
        ] == [
            ("linear", "BTC", "USD", "base_asset"),
            ("linear", "BTC", "USD", "base_asset"),
            ("inverse", "USD", "BTC", "contract_count"),
            (None, None, None, None),
        ]
        assert all(row.unit_certified for row in rows)


@pytest.mark.parametrize("native_unit", ("shares", "", "BASE_ASSET"))
def test_orm_rejects_unknown_native_order_quantity_units(
    payoff_sync_repo: DatabaseRepository, native_unit: str
) -> None:
    """Reject unsupported native unit labels independently of reconciliation metadata.

    Given: A native order convention outside the documented nullable enum,
    When: A writer persists an instrument specification,
    Then: The database refuses the unsupported value instead of defaulting its meaning.
    """
    with payoff_sync_repo.get_session() as session, pytest.raises(IntegrityError):
        DatabaseRepository.revise_instrument_spec_sync(
            session=session,
            instrument_public_id=_INSTRUMENT_ID,
            session_id=_SESSION_ID,
            sequence_id=1,
            timestamp=_FIRST,
            spec=InstrumentSpecInput(native_order_quantity_unit=native_unit),
        )
        session.flush()


def test_orm_rejects_partial_and_unknown_payoff_interpretations(
    payoff_sync_repo: DatabaseRepository,
) -> None:
    """The ORM schema enforces the same atomic, known-model contract as the migration.

    Given: Unknown payoff labels or incomplete currency evidence,
    When: A repository writer attempts to persist each invalid interpretation,
    Then: The database rejects the entire invalid monetary tuple.
    """
    invalid: tuple[tuple[str | None, str | None, str | None], ...] = (
        ("quanto", "BTC", "USD"),
        ("linear", None, None),
        (None, "BTC", None),
        (None, None, "USD"),
        ("linear", "BTC", None),
        ("linear", None, "USD"),
        (None, "BTC", "USD"),
        ("linear", "", "USD"),
        ("inverse", "USD", " "),
    )
    with payoff_sync_repo.get_session() as session:
        for sequence, evidence in enumerate(invalid, start=1):
            with pytest.raises(IntegrityError), session.begin_nested():
                DatabaseRepository.revise_instrument_spec_sync(
                    session=session,
                    instrument_public_id=_INSTRUMENT_ID,
                    session_id=_SESSION_ID,
                    sequence_id=sequence,
                    timestamp=_FIRST,
                    spec=_spec(evidence, _FIRST),
                )
