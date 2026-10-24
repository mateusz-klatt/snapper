"""Bind actual Futures source children to real execution rows and immutable native units."""

from dataclasses import dataclass
from datetime import datetime

from pydantic import BaseModel
from pydantic import ConfigDict
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from snapper.application.trade.private_fill_source import FuturesPrivateFill
from snapper.application.trade.private_fill_source import parse_futures_private_fill
from snapper.core.contract_accounting_observation import RawAccountingObservation
from snapper.core.execution_route_authority import ExecutionRouteAuthorityPayload
from snapper.core.execution_route_authority import client_selection_digest
from snapper.core.execution_route_authority import client_selection_from_authority
from snapper.core.execution_unit_snapshot import EvidenceUUID
from snapper.core.execution_unit_snapshot import ExecutionUnitSnapshotPayload
from snapper.core.execution_unit_snapshot import FillSnapshotTarget
from snapper.core.execution_unit_snapshot import SnapshotDimensions
from snapper.core.execution_unit_snapshot import SnapshotReceipt
from snapper.core.execution_unit_snapshot import SnapshotScope
from snapper.core.execution_unit_snapshot import SnapshotVenueIdentity
from snapper.core.execution_unit_snapshot import SnapshotWriteProvenance
from snapper.data.accounting_observation_binding import get_bound_accounting_observation
from snapper.data.execution_route_authority_store import get_bound_route_authority
from snapper.data.execution_unit_admission_store import load_original_submission_snapshot
from snapper.data.execution_unit_catalog import UnitCatalogLookup
from snapper.data.execution_unit_catalog import UnitCatalogObservation
from snapper.data.execution_unit_catalog import catalog_economic_context
from snapper.data.execution_unit_catalog import catalog_rebase_context
from snapper.data.execution_unit_catalog import load_unit_catalog
from snapper.data.execution_unit_catalog import snapshot_catalog
from snapper.data.execution_unit_snapshot_store import SnapshotBindingError
from snapper.data.execution_unit_snapshot_store import SnapshotWriteResult
from snapper.data.execution_unit_snapshot_store import _insert_bound_snapshot
from snapper.data.execution_unit_snapshot_store import bound_execution_digest
from snapper.data.execution_unit_snapshot_store import bound_venue_event_digest
from snapper.data.models import AccountingRawReceipt
from snapper.data.models import Execution
from snapper.data.models import ExecutionRouteAuthority
from snapper.data.models import SymbolAlias
from snapper.data.models import TradeCommand
from snapper.data.models import VenueEvent


class FuturesFillSourceRequest(BaseModel):
    """Actual retained receipt and historical route before any financial row is created."""

    model_config = ConfigDict(
        extra="forbid", frozen=True, strict=True, revalidate_instances="always"
    )

    wallet_public_id: EvidenceUUID
    raw_receipt_public_id: EvidenceUUID
    route_authority_public_id: EvidenceUUID


class FillUnitCaptureRequest(FuturesFillSourceRequest):
    """Actual existing financial row identities within the caller's fenced transaction."""

    execution_public_id: EvidenceUUID
    venue_event_public_id: EvidenceUUID


@dataclass(frozen=True)
class BoundFuturesFillSource:
    """Actual retained source, historical route and exact provider-to-instrument lineage."""

    raw: RawAccountingObservation
    authority: ExecutionRouteAuthorityPayload
    fill: FuturesPrivateFill
    alias: SymbolAlias
    catalog: UnitCatalogObservation


@dataclass(frozen=True)
class CapturedFillUnitEvidence:
    """A flushed immutable witness with the actual source required by the position writer."""

    request: FillUnitCaptureRequest
    source: BoundFuturesFillSource
    payload: ExecutionUnitSnapshotPayload
    result: SnapshotWriteResult


def _require(condition: bool, reason: str) -> None:
    """Abort financial interpretation while the separately committed raw receipt remains held."""
    if not condition:
        raise SnapshotBindingError(reason)


def _verify_source_route(
    request: FuturesFillSourceRequest,
    raw: RawAccountingObservation,
    authority: ExecutionRouteAuthorityPayload,
    fill: FuturesPrivateFill,
) -> None:
    """Require the same actual client revision without demanding current authority freshness."""
    selection = client_selection_from_authority(authority)
    _require(
        (
            request.wallet_public_id,
            raw.scope.wallet_public_id,
            raw.scope.exchange,
            raw.scope.mode,
            raw.scope.environment,
            raw.credential_public_id,
            raw.credential_timestamp,
            raw.route_revision,
        )
        == (
            selection.wallet_public_id,
            selection.wallet_public_id,
            selection.exchange,
            selection.mode,
            selection.environment,
            selection.credential_public_id,
            selection.credential_timestamp,
            client_selection_digest(selection),
        ),
        "fill_actual_client_scope_mismatch",
    )
    _require(
        raw.route_authority_public_id in {None, request.route_authority_public_id}
        and raw.scope.margin_account in {None, authority.scope.margin_account},
        "fill_actual_account_scope_mismatch",
    )
    _require(
        authority.venue_account_id is None or fill.venue_account_id == authority.venue_account_id,
        "fill_known_venue_account_mismatch",
    )
    _require(
        selection.credential_timestamp is None or selection.credential_timestamp <= raw.observed_at,
        "fill_selected_credential_from_future",
    )


async def _require_retained_source_knowledge(
    session: AsyncSession, request: FuturesFillSourceRequest, knowledge_at: datetime
) -> None:
    """Refuse source or account records first retained after execution's local knowledge clock."""
    raw_known_at = (
        await session.execute(
            select(AccountingRawReceipt.timestamp).where(
                AccountingRawReceipt.public_id == request.raw_receipt_public_id
            )
        )
    ).scalar_one()
    route_known_at = (
        await session.execute(
            select(ExecutionRouteAuthority.timestamp).where(
                ExecutionRouteAuthority.public_id == request.route_authority_public_id
            )
        )
    ).scalar_one()
    _require(raw_known_at <= knowledge_at, "fill_raw_receipt_not_known_at_execution")
    _require(route_known_at <= knowledge_at, "fill_route_authority_not_known_at_execution")


async def bind_futures_fill_source(
    session: AsyncSession, request: FuturesFillSourceRequest, knowledge_at: datetime
) -> BoundFuturesFillSource:
    """Establish exact source and native units from immutable receipts and actual historical rows.

    Args:
        session: Caller-owned financial transaction holding the wallet fence.
        request: Actual durable source and target identities.
        knowledge_at: Actual local observation/transaction clock, distinct from venue execution time.

    Returns:
        Source proof without committing, posting or resolving its hold.
    """
    request = FuturesFillSourceRequest(
        wallet_public_id=request.wallet_public_id,
        raw_receipt_public_id=request.raw_receipt_public_id,
        route_authority_public_id=request.route_authority_public_id,
    )
    raw = await get_bound_accounting_observation(session, request.raw_receipt_public_id)
    _require(knowledge_at >= raw.observed_at, "fill_knowledge_precedes_raw_observation")
    authority = await get_bound_route_authority(session, request.route_authority_public_id)
    _require(authority.observed_at <= knowledge_at, "fill_route_authority_not_known_at_execution")
    await _require_retained_source_knowledge(session, request, knowledge_at)
    fill = parse_futures_private_fill(raw)
    _verify_source_route(request, raw, authority, fill)
    alias = (
        await session.execute(
            select(SymbolAlias).where(
                SymbolAlias.exchange == "kraken_futures",
                SymbolAlias.channel == "ws",
                SymbolAlias.exchange_symbol == fill.provider_symbol,
                SymbolAlias.timestamp <= knowledge_at,
                SymbolAlias.known_to > knowledge_at,
            )
        )
    ).scalar_one_or_none()
    if alias is None:
        raise SnapshotBindingError("fill_historical_provider_alias_missing")
    catalog = await load_unit_catalog(
        session,
        UnitCatalogLookup(exchange="kraken_futures", symbol_public_id=alias.symbol_public_id),
        knowledge_at,
    )
    catalog_economic_context(catalog)
    return BoundFuturesFillSource(raw, authority, fill, alias, catalog)


def futures_fill_receipt(source: BoundFuturesFillSource) -> SnapshotReceipt:
    """Retain the whole original frame with the verified native Futures quantity convention.

    Args:
        source: Actual provider alias and verified historical Futures definition.

    Returns:
        V1 receipt; the separate raw receipt identity preserves the exact selected child.
    """
    context = catalog_economic_context(source.catalog)
    return SnapshotReceipt(
        representation="wire_bytes_base64",
        material=source.raw.material,
        endpoint_or_channel=source.raw.channel,
        observed_at=source.raw.observed_at,
        reported_quantity_unit=context.native_order_quantity_unit,
        reported_quantity_convention="not_applicable",
    )


def _dimensions(source: BoundFuturesFillSource) -> SnapshotDimensions:
    """Copy actual executed operands without constructing a fictitious submission command."""
    context = catalog_economic_context(source.catalog)
    fill = source.fill
    return SnapshotDimensions.model_validate(
        {
            **context.model_dump(exclude={"quantity_convention", "instrument_kind", "expiry_at"}),
            "signed_quantity_unit": context.native_order_quantity_unit,
            "side": fill.side,
            "signed_quantity": fill.quantity if fill.side == "buy" else fill.quantity.copy_negate(),
            "price": fill.price,
            "fee": fill.fee,
            "fee_currency": fill.fee_currency,
        }
    )


def _verify_actual_execution(source: BoundFuturesFillSource, execution: Execution) -> None:
    """Require the actual execution to represent precisely this source child, not a cumulative gap."""
    fill = source.fill
    _require(
        execution.exec_id == fill.execution_id and execution.executed_at == fill.occurred_at,
        "fill_actual_execution_source_identity_mismatch",
    )
    _require(
        execution.price_decimal is not None
        and execution.size_decimal is not None
        and execution.fee_decimal is not None,
        "fill_actual_execution_raw_operands_missing",
    )


async def _require_admitted_units(
    session: AsyncSession, source: BoundFuturesFillSource, event: VenueEvent, knowledge_at: datetime
) -> None:
    """Prevent delayed fills from borrowing economically changed catalog units at observation time."""
    command = (
        await session.execute(
            select(TradeCommand).where(
                TradeCommand.public_id == event.command_public_id,
                TradeCommand.timestamp <= knowledge_at,
                TradeCommand.known_to > knowledge_at,
            )
        )
    ).scalar_one_or_none()
    if command is None:
        raise SnapshotBindingError("fill_admitted_command_missing")
    original = await load_original_submission_snapshot(session, command)
    if original is None:
        raise SnapshotBindingError("fill_admitted_unit_snapshot_missing")
    _require(
        original.payload.scope.instrument_public_id == source.catalog.instrument.public_id,
        "fill_admitted_instrument_mismatch",
    )
    original_catalog = await load_unit_catalog(
        session,
        UnitCatalogLookup(
            exchange="kraken_futures", native_symbol=original.payload.venue_identity.native_symbol
        ),
        original.first_known,
    )
    _require(
        catalog_economic_context(original_catalog) == catalog_economic_context(source.catalog),
        "fill_admitted_economic_units_changed",
    )


async def capture_bound_futures_fill_units(
    session: AsyncSession,
    request: FillUnitCaptureRequest,
    provenance: SnapshotWriteProvenance,
) -> CapturedFillUnitEvidence:
    """Flush verified fill units into the same transaction as execution, postings and outbox.

    Args:
        session: Caller-owned financial transaction with the wallet fence held.
        request: Actual raw receipt, route, execution and source-event identities.
        provenance: Snapshot insertion identity and real interpretation knowledge clock.

    Returns:
        Verified source and flushed immutable units; the caller must still commit all effects.
    """
    request = FillUnitCaptureRequest.model_validate(request)
    provenance = SnapshotWriteProvenance.model_validate(provenance)
    execution = (
        await session.execute(
            select(Execution).where(Execution.public_id == request.execution_public_id)
        )
    ).scalar_one_or_none()
    event = (
        await session.execute(
            select(VenueEvent).where(VenueEvent.public_id == request.venue_event_public_id)
        )
    ).scalar_one_or_none()
    if execution is None or event is None:
        raise SnapshotBindingError("fill_actual_target_missing")
    source = await bind_futures_fill_source(session, request, execution.timestamp)
    _verify_actual_execution(source, execution)
    _require(event.exchange_order_id == source.fill.order_id, "fill_actual_order_identity_mismatch")
    _require(
        source.fill.client_order_id is None or event.venue_client_id == source.fill.client_order_id,
        "fill_actual_client_order_identity_mismatch",
    )
    _require(event.venue_timestamp == source.fill.occurred_at, "fill_actual_venue_time_mismatch")
    await _require_admitted_units(session, source, event, execution.timestamp)
    payload = ExecutionUnitSnapshotPayload(
        target=FillSnapshotTarget(
            execution_public_id=execution.public_id,
            execution_digest=bound_execution_digest(execution),
            venue_event_public_id=event.public_id,
            venue_event_digest=bound_venue_event_digest(event),
        ),
        scope=SnapshotScope(
            wallet_public_id=request.wallet_public_id,
            exchange="kraken_futures",
            mode=source.raw.scope.mode,
            instrument_public_id=source.catalog.instrument.public_id,
        ),
        catalog=snapshot_catalog(source.catalog),
        dimensions=_dimensions(source),
        venue_identity=SnapshotVenueIdentity(
            native_symbol=source.catalog.symbol.native_symbol,
            source="kraken.futures.websocket.fills",
            source_version="1",
            observed_at=source.raw.observed_at,
            wsname=source.fill.provider_symbol,
        ),
        rebase=catalog_rebase_context(source.catalog),
        receipt=futures_fill_receipt(source),
        interpreted_at=provenance.timestamp,
    )
    result = await _insert_bound_snapshot(session, payload, provenance)
    return CapturedFillUnitEvidence(request, source, payload, result)
