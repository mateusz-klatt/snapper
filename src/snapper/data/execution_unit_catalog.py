"""Real temporal catalog observations shared by unit producers and proof consumers."""

from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from snapper.application.portfolio.spot_precision_certification import KRAKEN_INSTRUMENT_SPEC_SOURCE
from snapper.application.updaters.symbols.polygon_cash_profile import POLYGON_CASH_PROFILE_SOURCE
from snapper.core.execution_unit_admission import EconomicUnitContext
from snapper.core.execution_unit_admission import InstrumentSourceObservation
from snapper.core.execution_unit_snapshot import SnapshotCatalog
from snapper.core.execution_unit_snapshot import SnapshotCommandMaterial
from snapper.core.execution_unit_snapshot import SnapshotDimensions
from snapper.core.execution_unit_snapshot import SnapshotRebaseContext
from snapper.data.execution_unit_snapshot_store import SnapshotBindingError
from snapper.data.models import Instrument
from snapper.data.models import InstrumentSpec
from snapper.data.models import Symbol
from snapper.data.models import is_open_ended_version
from snapper.data.temporal_selection import one_or_none
from snapper.infrastructure.exchanges.futures_spec_provenance import FUTURES_SPEC_SOURCE

_QUOTE_PER_BASE_SOURCES = frozenset(
    {KRAKEN_INSTRUMENT_SPEC_SOURCE, POLYGON_CASH_PROFILE_SOURCE, FUTURES_SPEC_SOURCE}
)


@dataclass(frozen=True)
class UnitCatalogLookup:
    """An actual execution symbol or an explicitly selected source-symbol identity."""

    exchange: str
    native_symbol: str | None = None
    symbol_public_id: str | None = None

    def __post_init__(self) -> None:
        """Require exactly one explicit lookup key rather than guessing a source."""
        if (self.native_symbol is None) == (self.symbol_public_id is None):
            raise ValueError("A unit catalog lookup requires exactly one symbol identity")


@dataclass(frozen=True)
class UnitCatalogObservation:
    """The actual target/source instrument, symbol and specification row versions."""

    instrument: Instrument
    symbol: Symbol
    spec: InstrumentSpec


def catalog_decimal_mirror(value: float | None) -> Decimal | None:
    """Copy an existing float mirror without claiming an original venue decimal lexeme.

    Args:
        value: Actual persisted catalog float, or explicit absence.

    Returns:
        The decimalized mirror, retaining absence and refusing no values by inference.
    """
    return Decimal(str(value)) if value is not None else None


async def load_unit_catalog(
    session: AsyncSession,
    lookup: UnitCatalogLookup,
    as_of: datetime,
    current: bool = False,
) -> UnitCatalogObservation:
    """Select real temporal observations without reviving superseded current units.

    Args:
        session: Caller-owned transaction and existing execution fence.
        lookup: Explicit venue and native symbol or source-symbol UUID.
        as_of: Actual knowledge horizon to bind.
        current: Require physically current rows for actual submission admission.

    Returns:
        One unambiguous actual instrument/symbol/specification lineage.
    """
    statement = (
        select(
            Instrument,
            Symbol,
            InstrumentSpec,
            Instrument.known_to,
            Symbol.known_to,
            InstrumentSpec.known_to,
        )
        .join(Symbol, Symbol.public_id == Instrument.symbol_public_id)
        .join(InstrumentSpec, InstrumentSpec.instrument_public_id == Instrument.public_id)
        .where(
            Instrument.exchange == lookup.exchange,
            Instrument.timestamp <= as_of,
            Symbol.timestamp <= as_of,
            InstrumentSpec.timestamp <= as_of,
        )
    )
    if lookup.native_symbol is not None:
        statement = statement.where(Symbol.native_symbol == lookup.native_symbol)
    else:
        statement = statement.where(Symbol.public_id == lookup.symbol_public_id)
    statement = statement.where(
        Instrument.known_to > as_of,
        Symbol.known_to > as_of,
        InstrumentSpec.known_to > as_of,
    )
    candidates = (await session.execute(statement)).all()
    row = one_or_none(
        [
            (instrument, symbol, spec)
            for instrument, symbol, spec, instrument_end, symbol_end, spec_end in candidates
            if not current
            or all(
                is_open_ended_version(known_to)
                for known_to in (instrument_end, symbol_end, spec_end)
            )
        ]
    )
    if row is None:
        raise SnapshotBindingError("unit_catalog_lineage_missing")
    instrument, symbol, spec = row
    return UnitCatalogObservation(instrument, symbol, spec)


async def load_unit_source_catalog(
    session: AsyncSession,
    target: UnitCatalogObservation,
    as_of: datetime,
    current: bool = False,
) -> UnitCatalogObservation:
    """Resolve Paper's explicitly persisted source through real historical UUID lineage.

    Args:
        session: Caller-owned transaction holding the target account fence.
        target: Actual execution-target catalog observation.
        as_of: Intent or actual submission knowledge horizon.
        current: Require the actual currently selected source lineage.

    Returns:
        The real source catalog; live executions retain their own instrument identity.
    """
    if target.instrument.exchange != "paper":
        return target
    source = target.instrument.source_exchange
    if source is None or source == "paper":
        raise SnapshotBindingError("paper_source_instrument_identity_unavailable")
    return await load_unit_catalog(
        session,
        UnitCatalogLookup(exchange=source, symbol_public_id=target.instrument.symbol_public_id),
        as_of,
        current,
    )


def snapshot_catalog(observation: UnitCatalogObservation) -> SnapshotCatalog:
    """Copy exact specification provenance rather than a current-cache assertion.

    Args:
        observation: Real temporal instrument definition selected by the caller.

    Returns:
        Strict source, revision and provider observation clocks from the stored row.
    """
    spec = observation.spec
    return SnapshotCatalog.model_validate(
        {
            "spec_public_id": spec.public_id,
            "spec_timestamp": spec.timestamp,
            "source": spec.spec_source,
            "version": spec.spec_version,
            "observed_at": spec.spec_observed_at,
        }
    )


def source_catalog_observation(observation: UnitCatalogObservation) -> InstrumentSourceObservation:
    """Copy exact source identities separately from a simulated target's stable UUID.

    Args:
        observation: Real source instrument/symbol/specification lineage.

    Returns:
        Immutable source identity and exact catalog clocks for the v2 proof graph.
    """
    return InstrumentSourceObservation(
        exchange=observation.instrument.exchange,
        instrument_public_id=observation.instrument.public_id,
        instrument_timestamp=observation.instrument.timestamp,
        symbol_public_id=observation.symbol.public_id,
        symbol_timestamp=observation.symbol.timestamp,
        catalog=snapshot_catalog(observation),
    )


def catalog_rebase_context(observation: UnitCatalogObservation) -> SnapshotRebaseContext:
    """Keep unimplemented tokenized rebase conventions honestly unknown.

    Args:
        observation: Exact real catalog definition.

    Returns:
        A known non-tokenized convention only for an explicitly supported source profile.
    """
    return SnapshotRebaseContext(
        quantity_convention=(
            "not_applicable"
            if observation.spec.spec_source in _QUOTE_PER_BASE_SOURCES
            else "unknown"
        )
    )


def catalog_submission_dimensions(
    material: SnapshotCommandMaterial, observation: UnitCatalogObservation
) -> SnapshotDimensions:
    """Copy exact submitted operands and independently observed catalog units.

    Args:
        material: Actual durable request operands mirrored by the outgoing frame.
        observation: Exact intent-time or current catalog observation.

    Returns:
        Typed unit evidence without guessing unknown price or rebase conventions.
    """
    spec = observation.spec
    priced = spec.spec_source in _QUOTE_PER_BASE_SOURCES
    signed = material.quantity if material.side == "buy" else material.quantity.copy_negate()
    return SnapshotDimensions.model_validate(
        {
            "base_currency": observation.symbol.base,
            "quote_currency": observation.symbol.quote,
            "native_order_quantity_unit": spec.native_order_quantity_unit,
            "internal_quantity_unit": spec.quantity_unit,
            "signed_quantity_unit": spec.native_order_quantity_unit,
            "side": material.side,
            "signed_quantity": signed,
            "quantity_quantum": catalog_decimal_mirror(spec.lot_size),
            "price": material.price,
            "stop_price": material.stop_price,
            "price_quantum": catalog_decimal_mirror(spec.tick_size),
            "price_currency": observation.symbol.quote if priced else None,
            "price_quantity_convention": "quote_per_base" if priced else "unknown",
            "catalog_contract_multiplier": spec.contract_size,
            "payoff_model": spec.contract_payoff_model,
            "contract_multiplier": (
                spec.contract_size if spec.contract_payoff_model is not None else None
            ),
            "contract_value_currency": spec.contract_value_currency,
            "settlement_currency": spec.settlement_currency,
        }
    )


def catalog_economic_context(observation: UnitCatalogObservation) -> EconomicUnitContext:
    """Require known economic conventions before treating revision differences as benign.

    Args:
        observation: Exact real catalog definition, never a guessed alias interpretation.

    Returns:
        Complete operand-independent dimensions used by admission equality.
    """
    spec = observation.spec
    if spec.spec_source not in _QUOTE_PER_BASE_SOURCES:
        raise SnapshotBindingError("catalog_price_or_rebase_convention_unproven")
    if (spec.spec_source == FUTURES_SPEC_SOURCE) != (spec.contract_payoff_model is not None):
        raise SnapshotBindingError("catalog_cash_or_derivative_payoff_unproven")
    if spec.spec_source == FUTURES_SPEC_SOURCE and not spec.unit_certified:
        raise SnapshotBindingError("catalog_derivative_units_not_certified")
    return EconomicUnitContext.model_validate(
        {
            "base_currency": observation.symbol.base,
            "quote_currency": observation.symbol.quote,
            "native_order_quantity_unit": spec.native_order_quantity_unit,
            "internal_quantity_unit": spec.quantity_unit,
            "quantity_quantum": catalog_decimal_mirror(spec.lot_size),
            "price_quantum": catalog_decimal_mirror(spec.tick_size),
            "price_currency": observation.symbol.quote,
            "price_quantity_convention": "quote_per_base",
            "catalog_contract_multiplier": spec.contract_size,
            "payoff_model": spec.contract_payoff_model,
            "contract_multiplier": (
                spec.contract_size if spec.contract_payoff_model is not None else None
            ),
            "contract_value_currency": spec.contract_value_currency,
            "settlement_currency": spec.settlement_currency,
            "quantity_convention": "not_applicable",
            "instrument_kind": spec.instrument_kind,
            "expiry_at": spec.expiry_at,
        }
    )
